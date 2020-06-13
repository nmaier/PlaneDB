using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  public sealed partial class PlaneDB
  {
    private readonly BlockingCollection<bool> mergeRequests = new BlockingCollection<bool>(4);
    private readonly Thread? mergeThread;

    private void CompactLevels()
    {
      var newIds = new List<ulong>();

      const int LEVEL_TARGET_SIZE = BASE_TARGET_SIZE * 4;
      var mj = new UniqueMemoryJournal();
      var threadCount = Math.Max(2, Math.Min(3, Environment.ProcessorCount));
      var queue = new BlockingCollection<UniqueMemoryJournal>(threadCount);
      var flushThreads = Enumerable.Range(0, threadCount).Select(t => {
        var thread = new Thread(() => {
          foreach (var mjournal in queue.GetConsumingEnumerable()) {
            if (mjournal.IsEmpty) {
              continue;
            }

            var newId = manifest.AllocateIdentifier();

            var sst = FindFile(newId);

            using var builder =
              new SSTableBuilder(
                new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                options);
            mjournal.CopyTo(builder);
            lock (newIds) {
              newIds.Add(newId);
            }
          }
        }) { IsBackground = false, Name = $"Plane-CompactFlush-{t}" };
        thread.Start();
        return thread;
      }).ToArray();

      foreach (var item in this) {
        mj.Put(item.Key, item.Value);
        if (mj.Length < LEVEL_TARGET_SIZE) {
          continue;
        }

        queue.Add(mj);
        mj = new UniqueMemoryJournal();
      }

      if (!mj.IsEmpty) {
        queue.Add(mj);
      }

      queue.CompleteAdding();
      foreach (var flushThread in flushThreads) {
        flushThread.Join();
      }

      for (byte level = 0x0; level <= Math.Max((byte)0x1, manifest.HighestLevel); ++level) {
        if (level == 0x1) {
          manifest.CommitLevel(level, newIds.ToArray());
        }
        else {
          manifest.CommitLevel(level);
        }
      }

      ReopenSSTables();
      MaybeMergeInternal(true);
    }

    private void MaybeMerge(bool force = false)
    {
      if (options.ThreadSafe) {
        mergeRequests.TryAdd(force);
      }
      else {
        MaybeMergeInternal(force);
      }
    }

    private void MaybeMergeInternal(bool force = false)
    {
      for (byte level = 0x00; level < manifest.HighestLevel; ++level) {
        var maxFiles = force ? level < 2 ? 1 : 8 :
          level == 0 ? 8 : 16;

        KeyValuePair<ulong, SSTable>[] mergeSequence;
        bool needsTombstones;
        List<ulong> newUpper;
        rwlock.EnterReadLock();
        try {
          if (!manifest.TryGetLevelIds(level, out var ids) || ids.Length < maxFiles) {
            if (force && level < 2) {
              continue;
            }

            return;
          }

          manifest.TryGetLevelIds((byte)(level + 1), out var upperIds);
          newUpper = upperIds.ToList();

          ulong[] DropOneUpper()
          {
            if (newUpper.Count == 0) {
              return Array.Empty<ulong>();
            }

            var rv = newUpper[0];
            newUpper.RemoveAt(0);
            return new[] { rv };
          }

          mergeSequence = ids.Concat(DropOneUpper()).OrderByDescending(i => i)
            .Select(i => tables.First(t => t.Key == i)).ToArray();
          foreach (var kv in mergeSequence) {
            kv.Value.AddRef();
          }

          needsTombstones = newUpper.Count > 0 || level < manifest.HighestLevel - 1;
        }
        finally {
          rwlock.ExitReadLock();
        }

        try {
          var targetSize = BASE_TARGET_SIZE * (1 << (level + 1));

          var mj = new UniqueMemoryJournal();
          var threadCount = Math.Max(2, Math.Min(targetSize > 128 << 20 ? 2 : 5, Environment.ProcessorCount));
          var queue = new BlockingCollection<UniqueMemoryJournal>(threadCount);
          var flushThreads = Enumerable.Range(0, threadCount).Select(t => {
            var thread = new Thread(() => {
              foreach (var mjournal in queue.GetConsumingEnumerable()) {
                if (mjournal.IsEmpty) {
                  continue;
                }

                var newId = manifest.AllocateIdentifier();

                var sst = FindFile(newId);

                using var builder =
                  new SSTableBuilder(
                    new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                    options);
                mjournal.CopyTo(builder);
                lock (newUpper) {
                  newUpper.Add(newId);
                }
              }
            }) { IsBackground = false, Name = $"Plane-MergeFlush-{t}" };
            thread.Start();
            return thread;
          }).ToArray();

          foreach (var item in EnumerateSortedTables(mergeSequence.Select(i => i.Value.Enumerate()).ToArray(),
                                                     options.Comparer)) {
            if (item.Value == null) {
              if (needsTombstones) {
                mj.Remove(item.Key);
              }
            }
            else {
              mj.Put(item.Key, item.Value);
            }

            if (mj.Length < targetSize) {
              continue;
            }

            queue.Add(mj);
            mj = new UniqueMemoryJournal();
          }

          queue.Add(mj);
          queue.CompleteAdding();
          foreach (var flushThread in flushThreads) {
            flushThread.Join();
          }

          rwlock.EnterWriteLock();
          try {
            var gone = mergeSequence.Select(e => e.Key).ToHashSet();
            manifest.CommitLevel(
              (byte)(level + 1),
              manifest.TryGetLevelIds((byte)(level + 1), out var existing)
                ? existing.Where(i => !gone.Contains(i)).Concat(newUpper).OrderBy(i => i).ToArray()
                : newUpper.OrderBy(i => i).ToArray());

            if (manifest.TryGetLevelIds(level, out existing)) {
              manifest.CommitLevel(level, existing.Where(i => !gone.Contains(i)).ToArray());
            }
          }
          finally {
            rwlock.ExitWriteLock();
          }

          try {
            OnMergedTables?.Invoke(this, this);
          }
          catch {
            // ignored
          }
        }
        finally {
          foreach (var kv in mergeSequence) {
            kv.Value.Dispose();
          }

          rwlock.EnterWriteLock();
          try {
            ReopenSSTables();
          }
          finally {
            rwlock.ExitWriteLock();
          }
        }
      }
    }

    private void MergeLoop()
    {
      try {
        foreach (var force in mergeRequests.GetConsumingEnumerable()) {
          MaybeMergeInternal(force);
        }
      }
      catch {
        // ignored
      }
    }
  }
}
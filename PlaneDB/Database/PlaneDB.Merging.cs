using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NMaier.PlaneDB;

public sealed partial class PlaneDB
{
  private const int COMPACT_TARGET_FILES_LEVEL0 = 32;
  private const int COMPACT_TARGET_FILES_MAX = 64;
  private const int COMPACT_TARGET_FILES_NORMAL = 10;
  private const byte MAX_EXTENDED_LEVEL = 10;
  private const byte MAX_NORMAL_LEVEL = 5;
  private const byte SUPER_LEVEL = byte.MaxValue - 1;
  private readonly BlockingCollection<bool> mergeRequests = new(4);
  private readonly Thread? mergeThread;
  private readonly HashSet<IPlaneDBMergeParticipant<byte[], byte[]>> participants = [];

  /// <inheritdoc />
  public void RegisterMergeParticipant(
    IPlaneDBMergeParticipant<byte[], byte[]> participant)
  {
    lock (participants) {
      _ = participants.Add(participant);
    }
  }

  /// <inheritdoc />
  public void UnregisterMergeParticipant(
    IPlaneDBMergeParticipant<byte[], byte[]> participant)
  {
    lock (participants) {
      _ = participants.Remove(participant);
    }
  }

  private void BuildSuper()
  {
    var newId = state.AllocateIdentifier();
    var sst = state.FindFile(newId);
    var items = TryGetParticipantCollection(out var participant)
      ? this.Where(kv => !participant.IsDataStale(kv.Key, kv.Value))
      : this;
    var written = false;
    {
      using var builder = new SSTableBuilder(
        new FileStream(
          sst.FullName,
          FileMode.CreateNew,
          FileAccess.Write,
          FileShare.None,
          1),
        state.Salt,
        Options);
      foreach (var (key, value) in items) {
        builder.Put(key, value);
        written = true;
      }
    }

    if (written) {
      state.CommitLevel(family, SUPER_LEVEL, newId);
    }
    else {
      state.CommitLevel(family, SUPER_LEVEL);
      try {
        sst.Delete();
      }
      catch {
        // not needed
      }
    }

    for (byte level = 0x0; level < SUPER_LEVEL; ++level) {
      state.CommitLevel(family, level);
    }

    state.FlushManifest();
    ReopenSSTables();
    MaybeMergeInternal();
  }

  private void CompactLevels()
  {
    var desiredSize = CurrentRealSize / COMPACT_TARGET_FILES_NORMAL;
    var targetLevel = targetLevelSizes.Where(i => i.Key <= MAX_NORMAL_LEVEL)
      .Select(i => new KeyValuePair<byte, long>(i.Key, Math.Abs(i.Value - desiredSize)))
      .MinBy(i => i.Value)
      .Key;
    var targetSize = targetLevelSizes[targetLevel];

    var newIds = new SortedList<byte, List<ulong>>();
    var mj = new JournalUniqueMemory();
    var threadCount = Math.Max(2, Math.Min(3, Environment.ProcessorCount));
    var queue = new BlockingCollection<JournalUniqueMemory>(threadCount);
    var flushThreads = Enumerable.Range(0, threadCount)
      .Select(
        t => {
          var thread = new Thread(
            () => {
              foreach (var memJournal in queue.GetConsumingEnumerable()) {
                if (memJournal.IsEmpty) {
                  continue;
                }

                var length = memJournal.JournalLength;

                var newId = state.AllocateIdentifier();
                var sst = state.FindFile(newId);
                using var builder = new SSTableBuilder(
                  new FileStream(
                    sst.FullName,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    1),
                  state.Salt,
                  Options);
                memJournal.CopyTo(builder);
                // Find the appropriate level
                var finalLevel = targetLevel;
                while (finalLevel > 0 && length < targetLevelSizes[finalLevel]) {
                  finalLevel--;
                }

                lock (newIds) {
                  if (!newIds.TryGetValue(finalLevel, out var ids)) {
                    newIds[finalLevel] = [
                      newId
                    ];
                  }
                  else {
                    ids.Add(newId);
                  }
                }
              }
            }) {
            IsBackground = false,
            Name = $"Plane-CompactFlush-{Location.Name}-{TableSpace}-{t}"
          };
          thread.Start();

          return thread;
        })
      .ToArray();

    IEnumerable<KeyValuePair<byte[], byte[]>> items = this;
    if (TryGetParticipantCollection(out var participant)) {
      items = items.Where(kv => !participant.IsDataStale(kv.Key, kv.Value));
    }

    foreach (var (key, value) in items) {
      mj.Put(key, value);
      if (mj.JournalLength <= targetSize) {
        continue;
      }

      queue.Add(mj);
      mj = new JournalUniqueMemory();
    }

    if (!mj.IsEmpty) {
      queue.Add(mj);
    }

    queue.CompleteAdding();
    foreach (var flushThread in flushThreads) {
      flushThread.Join();
    }

    for (byte level = 0x0;
         level <= Math.Max(targetLevel, state.GetHighestLevel(family));
         ++level) {
      if (newIds.TryGetValue(level, out var list)) {
        state.CommitLevel(family, level, [.. list.OrderBy(i => i)]);
      }
      else {
        state.CommitLevel(family, level);
      }
    }

    state.FlushManifest();
    ReopenSSTables();
    MaybeMergeInternal();
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private void MaybeMerge()
  {
    if (Options.ThreadSafe) {
      _ = mergeRequests.TryAdd(true);
    }
    else {
      MaybeMergeInternal();
    }
  }

  [MethodImpl(MethodImplOptions.NoInlining)]
  private void MaybeMergeInternal()
  {
    // Already disposing, do not perform further merges.
    if (Interlocked.CompareExchange(ref disposed, 0, 0) != 0) {
      return;
    }

    for (byte level = 0x00;
         level <= Math.Min(state.GetHighestLevel(family), (byte)(MAX_EXTENDED_LEVEL - 1));
         ++level) {
      using var stack = new ExitStack();

      var nextLevel = (byte)(level + 1);
      var maxFiles = level switch {
        0 => COMPACT_TARGET_FILES_LEVEL0,
        <= MAX_NORMAL_LEVEL => COMPACT_TARGET_FILES_NORMAL,
        _ => COMPACT_TARGET_FILES_MAX
      };
      var targetSize = targetLevelSizes[nextLevel];
      var largeTargetSize = (long)(targetSize * 0.85);

      KeyValuePair<ulong, ISSTable>[] mergeSequence;
      var allocatedIds = new ConcurrentQueue<ulong>();
      var largeAllocatedIds = new ConcurrentQueue<ulong>();
      bool needsTombstones;
      using (state.ReadWriteLock.AcquireUpgradableLock()) {
        if (!state.TryGetLevelIds(family, level, out var ids) || ids.Length < maxFiles) {
          continue;
        }

        _ = state.TryGetLevelIds(family, (byte)(level + 1), out var upperIds);

        IEnumerable<ulong> GetOneUpper()
        {
          if (upperIds.Length == 0) {
            return [];
          }

          var rv = upperIds[0];

          return [
            rv
          ];
        }

        ulong[] GetLowerLevel(int lower)
        {
          return !state.TryGetLevelIds(family, (byte)lower, out var lowerIds)
            ? []
            : lowerIds;
        }

        IEnumerable<ulong> GetLower()
        {
          return Enumerable.Range(0, level).SelectMany(GetLowerLevel);
        }

        var mergeIds = ids.Concat(GetLower()).Concat(GetOneUpper()).ToHashSet();
        mergeSequence = state.Sequence(family)
          .Where(mergeIds.Contains)
          .Select(i => tables.First(t => t.Key == i))
          .Select(
            i => {
              i.Value.AddRef();
              _ = stack.Register(i.Value);

              return i;
            })
          .ToArray();

        needsTombstones =
          upperIds.Length > 1 || level < state.GetHighestLevel(family) - 1;
        using (state.ReadWriteLock.AcquireWriteLock()) {
          var neededIds = (int)Math.Ceiling(
                            mergeSequence.Select(i => i.Value.RealSize).Sum() *
                            1.0 /
                            targetSize) +
                          2;
          while (neededIds > 0) {
            allocatedIds.Enqueue(state.AllocateIdentifier());
            largeAllocatedIds.Enqueue(state.AllocateIdentifier());
            neededIds--;
          }

          state.FlushManifest();
        }
      }

      var mj = new JournalUniqueMemory();
      var threadCount = Math.Max(
        2,
        Math.Min(targetSize > 128 << 20 ? 2 : 5, Environment.ProcessorCount));
      var queue = new BlockingCollection<JournalUniqueMemory>(threadCount);
      var newIds = new SortedList<byte, List<ulong>>();

      var flushThreads = Enumerable.Range(0, threadCount)
        .Select(
          t => {
            var thread = new Thread(
              () => {
                foreach (var memJournal in queue.GetConsumingEnumerable()) {
                  if (memJournal.IsEmpty) {
                    continue;
                  }

                  var length = memJournal.JournalLength;
                  ulong newId;
                  if (memJournal.Count == 1) {
                    if (!largeAllocatedIds.TryDequeue(out newId)) {
                      if (!allocatedIds.TryDequeue(out newId)) {
                        newId = state.AllocateIdentifier();
                      }
                    }
                  }
                  else if (!allocatedIds.TryDequeue(out newId)) {
                    newId = state.AllocateIdentifier();
                  }

                  var sst = state.FindFile(newId);

                  using var builder = new SSTableBuilder(
                    new FileStream(
                      sst.FullName,
                      FileMode.CreateNew,
                      FileAccess.Write,
                      FileShare.None,
                      1),
                    state.Salt,
                    Options);
                  memJournal.CopyTo(builder);

                  // Find the appropriate level
                  var finalLevel = nextLevel;
                  while (finalLevel > 0 && length < targetLevelSizes[finalLevel] * 0.5) {
                    finalLevel--;
                  }

                  lock (newIds) {
                    if (!newIds.TryGetValue(finalLevel, out var ids)) {
                      newIds[finalLevel] = [
                        newId
                      ];
                    }
                    else {
                      ids.Add(newId);
                    }
                  }
                }
              }) {
              IsBackground = false,
              Name = $"Plane-MergeFlush-{Location.Name}-{TableSpace}-{t}"
            };
            thread.Start();

            return thread;
          })
        .ToArray();

      try {
        if (!TryGetParticipantCollection(out var participant)) {
          participant = new NullParticipant<byte[], byte[]>();
        }

        var items = mergeSequence.Select(i => i.Value.Enumerate())
          .ToArray()
          .EnumerateSortedUniquely(new KeyComparer(Options.Comparer));
        foreach (var (key, value) in items) {
          if (value == null || participant.IsDataStale(key, value)) {
            if (needsTombstones) {
              mj.Remove(key);
            }
          }
          else if (value.Length >= largeTargetSize) {
            var large = new JournalUniqueMemory();
            large.Put(key, value);
            queue.Add(large);

            continue;
          }
          else {
            mj.Put(key, value);
          }

          if (mj.JournalLength <= targetSize) {
            continue;
          }

          queue.Add(mj);
          mj = new JournalUniqueMemory();
        }

        queue.Add(mj);
      }
      finally {
        queue.CompleteAdding();
        foreach (var flushThread in flushThreads) {
          flushThread.Join();
        }
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        var gone = mergeSequence.Select(e => e.Key).ToHashSet();
        for (byte i = 0x0; i <= nextLevel; ++i) {
          ulong[] currentIds;
          currentIds = state.TryGetLevelIds(family, i, out currentIds)
            ? currentIds.Where(existing => !gone.Contains(existing)).ToArray()
            : [];

          if (newIds.TryGetValue(i, out var newIdList)) {
            currentIds = [.. currentIds.Concat(newIdList).OrderBy(id => id)];
          }

          state.CommitLevel(family, i, currentIds);
        }

        state.FlushManifest();
        ReopenSSTables();
      }

      try {
        OnMergedTables?.Invoke(this, this);
      }
      catch {
        // ignored
      }
    }
  }

  private void MaybeMergeSmallTail()
  {
    if (Options.ReadOnly || tables.Length <= 1) {
      return;
    }

    var available = Constants.LEVEL_SMALL_TAIL_SIZE;
    var mergers = new SortedList<ulong, ISSTable>(
      tables.TakeWhile(
          t => {
            available -= t.Value.RealSize;

            return available > 0;
          })
        .ToDictionary(i => i.Key, i => i.Value));
    if (mergers.Count <= 0) {
      return;
    }

    var merged = new MemoryTable(Options, 0);
    foreach (var i in mergers) {
      i.Value.CopyTo(merged);
    }

    try {
      if (Options.JournalEnabled) {
        merged.CopyTo(state);
        memoryTable = merged;

        return;
      }

      FlushTableUnlocked(merged);
    }
    finally {
      var keys = mergers.Keys.ToHashSet();
      for (byte level = 0x0; level <= state.GetHighestLevel(family); ++level) {
        if (!state.TryGetLevelIds(family, level, out var ids)) {
          continue;
        }

        var newIds = ids.Where(i => !keys.Contains(i)).ToArray();
        if (newIds.Length == ids.Length) {
          continue;
        }

        state.CommitLevel(family, level, [.. newIds.OrderBy(i => i)]);
      }

      ReopenSSTables();
    }
  }

  private void MergeLoop()
  {
    try {
      foreach (var _ in mergeRequests.GetConsumingEnumerable()) {
        MaybeMergeInternal();
      }
    }
    catch {
      // ignored
    }
  }

  private bool TryGetParticipantCollection(
    [MaybeNullWhen(false)] out IPlaneDBMergeParticipant<byte[], byte[]> participant)
  {
    lock (participants) {
      if (participants.Count == 0) {
        participant = null;

        return false;
      }

      participant = new ParticipantCollection<byte[], byte[]>(participants);

      return true;
    }
  }
}

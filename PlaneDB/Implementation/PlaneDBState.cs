using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NMaier.PlaneDB
{
  internal sealed class PlaneDBState : IDisposable
  {
    private readonly DirectoryInfo location;
    private readonly FileStream lockFile;
    private readonly PlaneDBOptions options;
    internal readonly IReadWriteLock ReadWriteLock;
    internal IJournal Journal;
    internal Manifest Manifest;
    private long refs = 1;

    internal PlaneDBState(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
    {
      this.location = location;
      this.options = options;
      ReadWriteLock = options.ThreadSafe ? options.TrueReadWriteLock : new FakeReadWriteLock();

      try {
        lockFile = mode switch {
          FileMode.CreateNew => new FileStream(Manifest.FindFile(location, options, Manifest.LOCK_FILE).FullName,
                                               FileMode.CreateNew, FileAccess.ReadWrite,
                                               FileShare.None),
          FileMode.Open => new FileStream(Manifest.FindFile(location, options, Manifest.LOCK_FILE).FullName,
                                          FileMode.Create, FileAccess.ReadWrite,
                                          FileShare.None),
          FileMode.OpenOrCreate => new FileStream(Manifest.FindFile(location, options, Manifest.LOCK_FILE).FullName,
                                                  FileMode.Create, FileAccess.ReadWrite,
                                                  FileShare.None),
          _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };
      }
      catch (UnauthorizedAccessException ex) {
        throw new AlreadyLockedException(ex);
      }
      catch (IOException ex) {
        throw new AlreadyLockedException(ex);
      }

      // ReSharper disable once ConvertSwitchStatementToSwitchExpression
      switch (mode) {
        case FileMode.CreateNew:
        case FileMode.Open:
        case FileMode.OpenOrCreate:
          Manifest = new Manifest(location, mode, options);
          break;
        case FileMode.Append:
        case FileMode.Create:
        case FileMode.Truncate:
          throw new NotSupportedException(nameof(mode));
        default:
          throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
      }

      Manifest.RemoveOrphans();
      MaybeReplayJournal(Manifest);

      Journal = OpenJournal();
    }

    public void Dispose()
    {
      if (Interlocked.Decrement(ref refs) != 0) {
        return;
      }

      Journal.Dispose();
      MaybeCompactManifest();
      Manifest.Dispose();
      lockFile.Dispose();
    }

    internal void ClearJournal()
    {
      Journal.Dispose();
      Journal = OpenJournal();
    }

    internal void MaybeCompactManifest()
    {
      if (Manifest.IsEmpty) {
        return;
      }

      var man = Manifest.File;
      var newman = Manifest.FindFile(Manifest.MANIFEST_FILE + "-NEW");
      var oldman = Manifest.FindFile(Manifest.MANIFEST_FILE + "-OLD");
      Manifest.Compact(new FileStream(newman.FullName, FileMode.Create, FileAccess.ReadWrite,
                                      FileShare.None, 4096));
      Manifest.Dispose();
      File.Move(man.FullName, oldman.FullName);
      File.Move(newman.FullName, man.FullName);
      Manifest = new Manifest(location, FileMode.Open, options);
      File.Delete(oldman.FullName);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MaybeReplayJournal(Manifest manifest)
    {
      if (!options.JournalEnabled) {
        return;
      }

      using var jbs =
        new FileStream(manifest.FindFile(Manifest.JOURNAL_FILE).FullName, FileMode.OpenOrCreate, FileAccess.Read,
                       FileShare.None, 16384);
      if (jbs.Length <= 0 || jbs.Length == 4) {
        return;
      }

      var newId = manifest.AllocateIdentifier();
      var sst = manifest.FindFile(newId);

      try {
        using var builder =
          new SSTableBuilder(new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                             options);
        NMaier.PlaneDB.Journal.ReplayOnto(jbs, options, builder);
        manifest.AddToLevel(Array.Empty<byte>(), 0x00, newId);
      }
      catch (BrokenJournalException) {
        try {
          sst.Delete();
        }
        catch {
          // ignored
        }

        if (!options.AllowSkippingOfBrokenJournal) {
          throw;
        }
      }
    }

    private IJournal OpenJournal()
    {
      if (options.JournalEnabled) {
        return new Journal(new FileStream(Manifest.FindFile(Manifest.JOURNAL_FILE).FullName, FileMode.Create,
                                          FileAccess.ReadWrite,
                                          FileShare.None, PlaneDB.BASE_TARGET_SIZE, FileOptions.SequentialScan),
                           options);
      }

      return new FakeJournal();
    }
  }
}
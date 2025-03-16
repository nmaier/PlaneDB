using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBState : IPlaneDBState
{
  private readonly DirectoryInfo location;
  private readonly FileStream? lockFile;
  private readonly PlaneOptions options;
  private IJournal journal;
  private Manifest manifest;
  private long refs = 1;

  internal PlaneDBState(DirectoryInfo location, PlaneOptions options)
  {
    this.location = location;
    this.options = options;

    switch (options.OpenMode) {
      case PlaneOpenMode.ExistingReadWrite:
        break;
      case PlaneOpenMode.CreateReadWrite:
      case PlaneOpenMode.ReadWrite:
      case PlaneOpenMode.Repair:
        location.Create();

        break;
      case PlaneOpenMode.Packed:
        throw new NotSupportedException("PlaneDBState does not support packed");
      case PlaneOpenMode.ReadOnly:
        throw new NotSupportedException("PlaneDBState does not support readonly");
      default:
        throw new ArgumentOutOfRangeException(nameof(options));
    }

    ReadWriteLock = !options.ThreadSafe
      ? new FakeReadWriteLock()
      : options.TrueReadWriteLock;

    if (options.OpenMode != PlaneOpenMode.Packed) {
      try {
        var lockFileInfo = Manifest.FindFile(location, options, Manifest.LOCK_FILE);
        lockFile = options.OpenMode switch {
          PlaneOpenMode.CreateReadWrite => new FileStream(
            lockFileInfo.FullName,
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None),
          PlaneOpenMode.ExistingReadWrite => new FileStream(
            lockFileInfo.FullName,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None),
          PlaneOpenMode.ReadWrite => new FileStream(
            lockFileInfo.FullName,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.None),
          PlaneOpenMode.Repair => new FileStream(
            lockFileInfo.FullName,
            FileMode.Open,
            FileAccess.ReadWrite,
            FileShare.None),
          PlaneOpenMode.Packed or PlaneOpenMode.ReadOnly =>
            throw new NotSupportedException(),
          _ => throw new ArgumentOutOfRangeException(nameof(options))
        };
      }
      catch (UnauthorizedAccessException ex) {
        throw new PlaneDBAlreadyLockedException(ex);
      }
      catch (IOException ex) when (ex is not PlaneDBReadOnlyException) {
        throw new PlaneDBAlreadyLockedException(ex);
      }
    }

    try {
      manifest = new Manifest(location, options);
      manifest.RemoveOrphans();
    }
    catch {
      lockFile?.Dispose();

      throw;
    }

    try {
      if (options.OpenMode != PlaneOpenMode.CreateReadWrite) {
        MaybeReplayJournal(manifest);
      }

      journal = OpenJournal();
    }
    catch {
      manifest.Dispose();
      lockFile?.Dispose();

      throw;
    }
  }

  public void Dispose()
  {
    if (Interlocked.Decrement(ref refs) != 0) {
      return;
    }

    journal.Dispose();
    MaybeCompactManifest();
    manifest.Dispose();
    lockFile?.Dispose();
  }

  public long JournalLength => journal.JournalLength;

  public void AddToLevel(byte[] name, byte level, ulong id)
  {
    manifest.AddToLevel(name, level, id);
  }

  public ulong AllocateIdentifier()
  {
    return manifest.AllocateIdentifier();
  }

  public void ClearManifest()
  {
    manifest.ClearManifest();
  }

  public void CommitLevel(byte[] name, byte level, params ulong[] items)
  {
    manifest.CommitLevel(name, level, items);
  }

  public FileInfo FindFile(ulong id)
  {
    return manifest.FindFile(id);
  }

  public void FlushManifest()
  {
    manifest.FlushManifest();
  }

  public SortedList<byte, ulong[]> GetAllLevels(byte[] name)
  {
    return manifest.GetAllLevels(name);
  }

  public byte GetHighestLevel(byte[] name)
  {
    return manifest.GetHighestLevel(name);
  }

  public byte[] Salt => manifest.Salt;

  public IEnumerable<ulong> Sequence(byte[] name)
  {
    return manifest.Sequence(name);
  }

  public bool TryGetLevelIds(byte[] name, byte level, out ulong[] ids)
  {
    return manifest.TryGetLevelIds(name, level, out ids);
  }

  public void ClearJournal()
  {
    journal.Dispose();
    journal = OpenJournal();
  }

  public void MaybeCompactManifest()
  {
    if (manifest.IsManifestEmpty) {
      return;
    }

    var manifestFile = manifest.ManifestFile;
    var newFile = manifest.FindFile(Manifest.MANIFEST_FILE + "-NEW");
    var oldFile = manifest.FindFile(Manifest.MANIFEST_FILE + "-OLD");
    manifest.CompactManifest(
      new FileStream(
        newFile.FullName,
        FileMode.Create,
        FileAccess.ReadWrite,
        FileShare.None,
        4096));
    manifest.Dispose();
    File.Move(manifestFile.FullName, oldFile.FullName);
    File.Move(newFile.FullName, manifestFile.FullName);
    manifest = new Manifest(
      location,
      options.WithOpenMode(PlaneOpenMode.ExistingReadWrite));
    File.Delete(oldFile.FullName);
  }

  public IPlaneReadWriteLock ReadWriteLock { get; }

  public void Flush()
  {
    journal.Flush();
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    journal.Put(key, value);
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    journal.Remove(key);
  }

  [MethodImpl(MethodImplOptions.NoInlining)]
  private void MaybeReplayJournal(Manifest inputManifest)
  {
    if (!options.JournalEnabled) {
      return;
    }

    using var jbs = new FileStream(
      inputManifest.FindFile(Manifest.JOURNAL_FILE).FullName,
      FileMode.OpenOrCreate,
      FileAccess.Read,
      FileShare.None,
      16384);
    if (jbs.Length is <= 0 or 4) {
      return;
    }

    var newId = inputManifest.AllocateIdentifier();
    var sst = inputManifest.FindFile(newId);

    try {
      using var builder = new SSTableBuilder(
        new FileStream(
          sst.FullName,
          FileMode.CreateNew,
          FileAccess.Write,
          FileShare.None,
          1),
        Salt,
        options);
      Journal.ReplayOnto(jbs, inputManifest.Salt, options, builder);
      inputManifest.AddToLevel([], 0x00, newId);
    }
    catch (PlaneDBBrokenJournalException) {
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
    return options.JournalEnabled
      ? new Journal(
        new FileStream(
          manifest.FindFile(Manifest.JOURNAL_FILE).FullName,
          FileMode.Create,
          FileAccess.ReadWrite,
          FileShare.None,
          40960,
          FileOptions.SequentialScan),
        manifest.Salt,
        options,
        ReadWriteLock)
      : new JournalFake();
  }
}

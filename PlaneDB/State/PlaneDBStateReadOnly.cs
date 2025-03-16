using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBStateReadOnly : IPlaneDBState
{
  private readonly JournalReadOnly journal;
  private readonly FileStream? lockFile;
  private readonly ManifestReadOnly manifest;
  private long refs = 1;

  internal PlaneDBStateReadOnly(DirectoryInfo location, PlaneOptions options)
  {
    if (options.OpenMode != PlaneOpenMode.ReadOnly) {
      throw new NotSupportedException(
        "PlaneDBStateReadonly only supports readonly databases");
    }

    try {
      var lockFileInfo = Manifest.FindFile(location, options, Manifest.LOCK_FILE);
      lockFile = new FileStream(
        lockFileInfo.FullName,
        FileMode.Open,
        FileAccess.Read,
        FileShare.Read);
    }
    catch (UnauthorizedAccessException ex) {
      throw new PlaneDBAlreadyLockedException(ex);
    }
    catch (IOException ex) when (ex is not PlaneDBReadOnlyException) {
      throw new PlaneDBAlreadyLockedException(ex);
    }

    manifest = new ManifestReadOnly(location, options);
    journal = new JournalReadOnly();
  }

  public void Dispose()
  {
    if (Interlocked.Decrement(ref refs) != 0) {
      return;
    }

    journal.Dispose();
    manifest.Dispose();
    lockFile?.Dispose();
  }

  public long JournalLength => journal.JournalLength;

  public void AddToLevel(byte[] name, byte level, ulong id)
  {
    throw new PlaneDBReadOnlyException();
  }

  public ulong AllocateIdentifier()
  {
    throw new PlaneDBReadOnlyException();
  }

  public void ClearManifest()
  {
    throw new PlaneDBReadOnlyException();
  }

  public void CommitLevel(byte[] name, byte level, params ulong[] items)
  {
    throw new PlaneDBReadOnlyException();
  }

  public FileInfo FindFile(ulong id)
  {
    return manifest.FindFile(id);
  }

  public void FlushManifest()
  {
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
    throw new PlaneDBReadOnlyException();
  }

  public void MaybeCompactManifest()
  {
  }

  public IPlaneReadWriteLock ReadWriteLock { get; } = new ReadOnlyLock();

  public void Flush()
  {
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    throw new PlaneDBReadOnlyException();
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    throw new PlaneDBReadOnlyException();
  }
}

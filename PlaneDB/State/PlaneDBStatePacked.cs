using System;
using System.Collections.Generic;
using System.IO;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBStatePacked : IPlaneDBState
{
  private static readonly IPlaneReadWriteLock readWriteLock = new ReadOnlyLock();
  private readonly FileInfo manifestFile;

  internal PlaneDBStatePacked(FileInfo packFile)
  {
    manifestFile = packFile;
    using var m = manifestFile.OpenRead();
    if (m.ReadInt32() != Constants.MAGIC) {
      throw new PlaneDBBadMagicException();
    }

    if (m.ReadInt32() != SSTable.TABLE_VERSION) {
      throw new PlaneDBBadMagicException("Wrong table version");
    }

    Salt = m.ReadFullBlock(Constants.SALT_BYTES);
  }

  public void Dispose()
  {
  }

  public long JournalLength => 0;

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
    return manifestFile;
  }

  public void FlushManifest()
  {
  }

  public SortedList<byte, ulong[]> GetAllLevels(byte[] name)
  {
    return [];
  }

  public byte GetHighestLevel(byte[] name)
  {
    return 0x0;
  }

  public byte[] Salt { get; }

  public IEnumerable<ulong> Sequence(byte[] name)
  {
    return [
      0UL
    ];
  }

  public bool TryGetLevelIds(byte[] name, byte level, out ulong[] ids)
  {
    throw new PlaneDBReadOnlyException();
  }

  public void ClearJournal()
  {
    throw new PlaneDBReadOnlyException();
  }

  public void MaybeCompactManifest()
  {
  }

  public IPlaneReadWriteLock ReadWriteLock => readWriteLock;

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

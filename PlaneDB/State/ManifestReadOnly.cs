using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace NMaier.PlaneDB;

internal sealed class ManifestReadOnly : IManifest
{
  private static FileStream OpenManifestStream(
    DirectoryInfo location,
    PlaneOptions options)
  {
    var manifestFileInfo = Manifest.FindFile(location, options, Manifest.MANIFEST_FILE);

    return new FileStream(
      manifestFileInfo.FullName,
      FileMode.Open,
      FileAccess.Read,
      FileShare.Read,
      4096);
  }

  private readonly SortedList<byte[], SortedList<byte, ulong[]>> levels =
    new(PlaneByteArrayComparer.Default);

  private readonly DirectoryInfo location;
  private readonly PlaneOptions options;
  private readonly Stream stream;

  internal ManifestReadOnly(DirectoryInfo location, PlaneOptions options) : this(
    location,
    OpenManifestStream(location, options),
    options)
  {
  }

  internal ManifestReadOnly(DirectoryInfo location, Stream stream, PlaneOptions options)
  {
    this.location = location;
    this.stream = stream;
    this.options = options;
    if (stream.Length == 0) {
      throw new PlaneDBReadOnlyException(
        "Attempt to open an empty/non-existent database in read-only mode");
    }

    _ = stream.Seek(0, SeekOrigin.Begin);
    if (stream.ReadInt32() != Constants.MAGIC) {
      throw new PlaneDBBadMagicException();
    }

    if (stream.ReadInt32() != Manifest.MANIFEST_VERSION) {
      throw new PlaneDBBadMagicException("Bad manifest version");
    }

    Salt = stream.ReadFullBlock(Constants.SALT_BYTES);
    var transformer = options.GetTransformerFor(Salt);

    _ = stream.ReadUInt64();

    var magic2Length = stream.ReadInt32();
    if (magic2Length is < 0 or > short.MaxValue) {
      throw new PlaneDBBadMagicException();
    }

    var magic2 = stream.ReadFullBlock(magic2Length);
    Span<byte> actual = stackalloc byte[1024];
    int alen;
    try {
      alen = transformer.UntransformBlock(magic2, actual);
    }
    catch {
      throw new PlaneDBBadMagicException();
    }

    if (alen != Constants.MagicBytes.Length ||
        !actual[..alen].SequenceEqual(Constants.MagicBytes)) {
      throw new PlaneDBBadMagicException();
    }

    for (;;) {
      var level = stream.ReadByte();
      if (level < 0) {
        break;
      }

      var count = stream.ReadInt32();
      var name = Array.Empty<byte>();
      if (count < 0) {
        count = count == int.MinValue ? 0 : -count;
        var nameLen = stream.ReadInt32();
        name = stream.ReadFullBlock(nameLen);
      }

      if (count == 0) {
        _ = GetLevel(name).Remove((byte)level);

        continue;
      }

      var items = Enumerable.Range(0, count)
        .Select(_ => stream.ReadUInt64())
        .OrderBy(i => i)
        .ToArray();
      EnsureLevel(name)[(byte)level] = items;
    }
  }

  public void Dispose()
  {
    stream.Dispose();
  }

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
    return FindFile($"{id:D4}");
  }

  public void FlushManifest()
  {
  }

  public SortedList<byte, ulong[]> GetAllLevels(byte[] name)
  {
    return new SortedList<byte, ulong[]>(
      GetLevel(name).ToDictionary(l => l.Key, l => l.Value.ToArray()));
  }

  public byte GetHighestLevel(byte[] name)
  {
    var lvl = GetLevel(name).Keys.LastOrDefault();

    return lvl;
  }

  public byte[] Salt { get; }

  public IEnumerable<ulong> Sequence(byte[] name)
  {
    return GetLevel(name).OrderBy(i => i.Key).SelectMany(i => i.Value.Reverse());
  }

  public bool TryGetLevelIds(byte[] name, byte level, out ulong[] ids)
  {
    if (!GetLevel(name).TryGetValue(level, out var myIds)) {
      ids = [];

      return false;
    }

    ids = [.. myIds];

    return true;
  }

  private SortedList<byte, ulong[]> EnsureLevel(byte[] name)
  {
    if (!levels.TryGetValue(name, out var rv)) {
      levels[name] = rv = [];
    }

    return rv;
  }

  private FileInfo FindFile(string filename)
  {
    return Manifest.FindFile(location, options, filename);
  }

  private SortedList<byte, ulong[]> GetLevel(byte[] name)
  {
    return levels.TryGetValue(name, out var rv) ? rv : [];
  }
}

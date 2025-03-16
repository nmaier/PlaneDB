using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

using static System.String;

namespace NMaier.PlaneDB;

internal sealed class Manifest : IManifest
{
  internal const string JOURNAL_FILE = "JOURNAL";
  internal const string LOCK_FILE = "LOCK";
  internal const string MANIFEST_FILE = "MANIFEST";
  internal const int MANIFEST_VERSION = 2;

  internal static FileInfo FindFile(
    DirectoryInfo location,
    PlaneOptions options,
    string filename)
  {
    var ts = IsNullOrEmpty(options.Tablespace) ? "default" : options.Tablespace;

    return new FileInfo(Path.Combine(location.FullName, $"{ts}-{filename}.planedb"));
  }

  private static FileStream OpenManifestStream(
    DirectoryInfo location,
    PlaneOptions options)
  {
    var manifestFileInfo = FindFile(location, options, MANIFEST_FILE);

    return options.OpenMode switch {
      PlaneOpenMode.ReadOnly => new FileStream(
        manifestFileInfo.FullName,
        FileMode.Open,
        FileAccess.Read,
        FileShare.Read,
        4096),
      PlaneOpenMode.CreateReadWrite => new FileStream(
        manifestFileInfo.FullName,
        FileMode.CreateNew,
        FileAccess.ReadWrite,
        FileShare.None,
        4096),
      PlaneOpenMode.ExistingReadWrite => new FileStream(
        manifestFileInfo.FullName,
        FileMode.Open,
        FileAccess.ReadWrite,
        FileShare.None,
        4096),
      PlaneOpenMode.ReadWrite => new FileStream(
        manifestFileInfo.FullName,
        FileMode.OpenOrCreate,
        FileAccess.ReadWrite,
        FileShare.None,
        4096),
      PlaneOpenMode.Repair => new FileStream(
        manifestFileInfo.FullName,
        FileMode.OpenOrCreate,
        FileAccess.ReadWrite,
        FileShare.None,
        4096),
      PlaneOpenMode.Packed => throw new ArgumentOutOfRangeException(nameof(options)),
      _ => throw new ArgumentOutOfRangeException(nameof(options))
    };
  }

  private readonly SortedList<byte[], SortedList<byte, ulong[]>> levels =
    new(PlaneByteArrayComparer.Default);

  private readonly DirectoryInfo location;
  private readonly PlaneOptions options;
  private readonly Stream stream;
  private ulong counter;

  internal Manifest(DirectoryInfo location, PlaneOptions options) : this(
    location,
    OpenManifestStream(location, options),
    null,
    options,
    0)
  {
  }

  internal Manifest(DirectoryInfo location, Stream stream, PlaneOptions options) : this(
    location,
    stream,
    null,
    options,
    0)
  {
  }

  private Manifest(
    DirectoryInfo location,
    Stream stream,
    byte[]? salt,
    PlaneOptions options,
    ulong counter)
  {
    this.location = location;
    this.counter = counter;
    this.stream = stream;
    this.options = options;
    if (stream.Length == 0) {
      Salt = InitEmpty(salt);

      return;
    }

    _ = stream.Seek(0, SeekOrigin.Begin);
    if (stream.ReadInt32() != Constants.MAGIC) {
      throw new PlaneDBBadMagicException();
    }

    if (stream.ReadInt32() != MANIFEST_VERSION) {
      throw new PlaneDBBadMagicException("Bad manifest version");
    }

    Salt = stream.ReadFullBlock(Constants.SALT_BYTES);
    var transformer = options.GetTransformerFor(Salt);

    this.counter = stream.ReadUInt64();

    var magic2Length = stream.ReadInt32();
    if (magic2Length is < 0 or > short.MaxValue) {
      throw new PlaneDBBadMagicException();
    }

    var magic2 = stream.ReadFullBlock(magic2Length);
    Span<byte> actualMagic2 = stackalloc byte[1024];
    int magic2Len;
    try {
      magic2Len = transformer.UntransformBlock(magic2, actualMagic2);
    }
    catch {
      throw new PlaneDBBadMagicException();
    }

    if (magic2Len != Constants.MagicBytes.Length ||
        !actualMagic2[..magic2Len].SequenceEqual(Constants.MagicBytes)) {
      throw new PlaneDBBadMagicException();
    }

    for (;;) {
      try {
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
      catch (Exception) {
        if (options.RepairMode) {
          break;
        }

        throw;
      }
    }
  }

  public bool IsManifestEmpty => levels.Count <= 0;
  public FileInfo ManifestFile => FindFile(MANIFEST_FILE);

  public void Dispose()
  {
    FlushManifest();
    stream.Dispose();
  }

  public void AddToLevel(byte[] name, byte level, ulong id)
  {
    var l = EnsureLevel(name);
    var items = !l.TryGetValue(level, out var val)
      ? l[level] = [
        id
      ]
      : l[level] = [
        .. val.Concat(
          [
            id
          ])
          .OrderBy(i => i)
      ];

    _ = stream.Seek(0, SeekOrigin.End);
    stream.WriteByte(level);
    stream.WriteInt32(name.Length > 0 ? -items.Length : items.Length);
    foreach (var item in items) {
      stream.WriteUInt64(item);
    }

    FlushManifest();
  }

  public ulong AllocateIdentifier()
  {
    lock (stream) {
      counter++;
      _ = stream.Seek((sizeof(int) * 2) + Constants.SALT_BYTES, SeekOrigin.Begin);
      stream.WriteUInt64(counter);
      stream.Flush();

      return counter;
    }
  }

  public void ClearManifest()
  {
    lock (this) {
      levels.Clear();
      _ = InitEmpty(Salt);
    }
  }

  public void CommitLevel(byte[] name, byte level, params ulong[] items)
  {
    lock (stream) {
      if (items.Length <= 0) {
        _ = stream.Seek(0, SeekOrigin.End);
        stream.WriteByte(level);
        stream.WriteInt32(name.Length > 0 ? int.MinValue : 0);
        if (name.Length > 0) {
          stream.WriteInt32(name.Length);
          stream.Write(name);
        }

        stream.Flush();
        _ = GetLevel(name).Remove(level);

        return;
      }

      items = items.OrderBy(i => i).Distinct().ToArray();
      _ = stream.Seek(0, SeekOrigin.End);
      stream.WriteByte(level);
      stream.WriteInt32(name.Length > 0 ? -items.Length : items.Length);
      if (name.Length > 0) {
        stream.WriteInt32(name.Length);
        stream.Write(name);
      }

      foreach (var item in items) {
        stream.WriteUInt64(item);
      }

      stream.Flush();
      EnsureLevel(name)[level] = items;
    }
  }

  public FileInfo FindFile(ulong id)
  {
    return FindFile($"{id:D4}");
  }

  public void FlushManifest()
  {
    switch (stream) {
      case FileStream fs:
        fs.Flush(true);

        break;
      default:
        stream.Flush();

        break;
    }
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

  public void CompactManifest(Stream destination)
  {
    if (destination.Length > 0) {
      _ = destination.Seek(0, SeekOrigin.Begin);
      destination.SetLength(0);
    }

    using var newManifest = new Manifest(location, destination, Salt, options, counter);
    foreach (var family in levels) {
      foreach (var level in family.Value.Where(level => level.Value.Length > 0)) {
        newManifest.CommitLevel(family.Key, level.Key, level.Value);
      }
    }
  }

  private SortedList<byte, ulong[]> EnsureLevel(byte[] name)
  {
    if (!levels.TryGetValue(name, out var rv)) {
      levels[name] = rv = [];
    }

    return rv;
  }

  public FileInfo FindFile(string filename)
  {
    return FindFile(location, options, filename);
  }

  private IEnumerable<ulong> FullSequence()
  {
    return levels.SelectMany(i => i.Value)
      .OrderBy(i => i.Key)
      .SelectMany(i => i.Value.Reverse());
  }

  private SortedList<byte, ulong[]> GetLevel(byte[] name)
  {
    return levels.TryGetValue(name, out var rv) ? rv : [];
  }

  private byte[] InitEmpty(byte[]? salt)
  {
    _ = stream.Seek(0, SeekOrigin.Begin);
    stream.WriteInt32(Constants.MAGIC);
    stream.WriteInt32(MANIFEST_VERSION);
    if (salt == null) {
      salt = new byte[Constants.SALT_BYTES];
      RandomNumberGenerator.Fill(salt);
    }

    stream.Write(salt);

    stream.WriteUInt64(counter);
    var trans = options.GetTransformerFor(salt);
    var transformed = trans.TransformBlock(Constants.MagicBytes);
    stream.WriteInt32(transformed.Length);
    stream.Write(transformed);
    stream.SetLength(stream.Position);
    stream.Flush();

    return [.. salt];
  }

  [MethodImpl(MethodImplOptions.NoInlining)]
  public void RemoveOrphans()
  {
    var orphans = FindOrphans().ToArray();
    foreach (var orphan in orphans) {
      try {
        orphan.Delete();
      }
      catch {
        // ignored
      }
    }

    return;

    IEnumerable<FileInfo> FindOrphans()
    {
      var valid = FullSequence().ToLookup(i => i);
      var ts = IsNullOrEmpty(options.Tablespace) ? "default" : options.Tablespace;
      var needle = new Regex(
        $"{Regex.Escape(options.Tablespace)}-(.*)\\.planedb",
        RegexOptions.Compiled);
      foreach (var fi in location.GetFiles(
                 $"{ts}-*.planedb",
                 SearchOption.TopDirectoryOnly)) {
        var m = needle.Match(fi.Name);
        if (!m.Success) {
          continue;
        }

        var name = m.Groups[1].Value;
        if (!ulong.TryParse(name, out var id)) {
          switch (name) {
            case JOURNAL_FILE:
            case LOCK_FILE:
            case MANIFEST_FILE:
              break;
            default:
              yield return fi;

              break;
          }

          continue;
        }

        if (valid.Contains(id)) {
          continue;
        }

        yield return fi;
      }
    }
  }
}

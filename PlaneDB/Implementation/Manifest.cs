using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using static System.String;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  internal sealed class Manifest : IDisposable
  {
    internal const string JOURNAL_FILE = "JOURNAL";
    internal const string LOCK_FILE = "LOCK";
    internal const string MANIFEST_FILE = "MANIFEST";

    internal static FileInfo FindFile(DirectoryInfo location, PlaneDBOptions options, string filename)
    {
      var ts = IsNullOrEmpty(options.TableSpace) ? "default" : options.TableSpace;
      return new FileInfo(Path.Combine(location.FullName, $"{ts}-{filename}.planedb"));
    }

    private static FileStream OpenManifestStream(DirectoryInfo location, PlaneDBOptions options, FileMode mode)
    {
      return new FileStream(FindFile(location, options, MANIFEST_FILE).FullName, mode, FileAccess.ReadWrite,
                            FileShare.None, 4096);
    }

    private readonly SortedList<byte[], SortedList<byte, ulong[]>> levels = new SortedList<byte[], SortedList<byte, ulong[]>>(new ByteArrayComparer());
    private readonly DirectoryInfo location;
    private readonly PlaneDBOptions options;
    private readonly Stream stream;
    private ulong counter;

    internal Manifest(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
      : this(location, OpenManifestStream(location, options, mode), options, 0)
    {
    }

    internal Manifest(DirectoryInfo location, Stream stream, PlaneDBOptions options)
      : this(location, stream, options, 0)
    {
    }

    private Manifest(DirectoryInfo location, Stream stream, PlaneDBOptions options, ulong counter)
    {
      this.location = location;
      this.counter = counter;
      this.stream = stream;
      this.options = options;
      if (stream.Length == 0) {
        InitEmpty();
        return;
      }

      stream.Seek(0, SeekOrigin.Begin);
      if (stream.ReadInt32() != Constants.MAGIC) {
        throw new IOException("Bad manifest magic");
      }

      this.counter = stream.ReadUInt64();

      var magic2Length = stream.ReadInt32();
      if (magic2Length < 0 || magic2Length > Int16.MaxValue) {
        throw new BadMagicException();
      }

      var magic2 = stream.ReadFullBlock(magic2Length);
      Span<byte> actual = stackalloc byte[1024];
      int alen;
      try {
        alen = options.BlockTransformer.UntransformBlock(magic2, actual);
      }
      catch {
        throw new BadMagicException();
      }

      if (alen != Constants.MagicBytes.Length || !actual.Slice(0, alen).SequenceEqual(Constants.MagicBytes)) {
        throw new BadMagicException();
      }


      for (;;) {
        var level = stream.ReadByte();
        if (level < 0) {
          break;
        }

        var count = stream.ReadInt32();
        byte[] name = Array.Empty<byte>();
        if (count < 0) {
          if (count == int.MinValue) {
            count = 0;
          }
          else {
            count = -count;
          }

          var namelen = stream.ReadInt32();
          name = stream.ReadFullBlock(namelen);
        }

        if (count == 0) {
          GetLevel(name).Remove((byte)level);
          continue;
        }

        var items = Enumerable.Range(0, count).Select(_ => stream.ReadUInt64()).OrderBy(i => i).ToArray();
        EnsureLevel(name)[(byte)level] = items;
      }
    }

    private SortedList<byte, ulong[]> GetLevel(byte[] name)
    {
      return levels.TryGetValue(name, out var rv) ? rv : new SortedList<byte, ulong[]>();
    }
    
    private SortedList<byte, ulong[]> EnsureLevel(byte[] name)
    {
      if (!levels.TryGetValue(name, out var rv)) {
        levels[name] = rv = new SortedList<byte, ulong[]>();
      }

      return rv;
    }

    internal byte GetHighestLevel(byte[] name) => GetLevel(name).Keys.LastOrDefault();

    public bool IsEmpty => levels.Count <= 0;

    internal SortedList<byte, ulong[]> GetAllLevels(byte[] name) =>
      new SortedList<byte, ulong[]>(GetLevel(name).ToDictionary(l => l.Key, l => l.Value.ToArray()));

    internal FileInfo File => FindFile(MANIFEST_FILE);

    public void Dispose()
    {
      switch (stream) {
        case FileStream fs:
          fs.Flush(options.MaxJournalActions < 0);
          break;
        default:
          stream.Flush();
          break;
      }

      stream.Dispose();
    }

    public void AddToLevel(byte[] name, byte level, ulong id)
    {
      ulong[] items;
      var l = EnsureLevel(name);
      if (!l.TryGetValue(level, out var val)) {
        items = l[level] = new[] { id };
      }
      else {
        items = l[level] = val.Concat(new[] { id }).OrderBy(i => i).ToArray();
      }

      stream.Seek(0, SeekOrigin.End);
      stream.WriteByte(level);
      stream.WriteInt32(name.Length > 0 ? -items.Length : items.Length);
      foreach (var item in items) {
        stream.WriteUInt64(item);
      }

      stream.Flush();
    }

    public void Clear()
    {
      lock (this) {
        levels.Clear();
        InitEmpty();
      }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public void RemoveOrphans()
    {
      IEnumerable<FileInfo> FindOrphans()
      {
        var valid = FullSequence().ToLookup(i => i);
        var ts = IsNullOrEmpty(options.TableSpace) ? "default" : options.TableSpace;
        var needle = new Regex($"{Regex.Escape(options.TableSpace)}-(.*)\\.planedb", RegexOptions.Compiled);
        foreach (var fi in location.GetFiles($"{ts}-*.planedb", SearchOption.TopDirectoryOnly)) {
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

      var orphans = FindOrphans().ToArray();
      foreach (var orphan in orphans) {
        try {
          orphan.Delete();
        }
        catch {
          // ignored
        }
      }
    }

    public bool TryGetLevelIds(byte[] name, byte level, out ulong[] ids)
    {
      if (!GetLevel(name).TryGetValue(level, out var myIds)) {
        ids = Array.Empty<ulong>();
        return false;
      }

      ids = myIds.ToArray();
      return true;
    }

    internal ulong AllocateIdentifier()
    {
      lock (stream) {
        counter++;
        stream.Seek(sizeof(int), SeekOrigin.Begin);
        stream.WriteUInt64(counter);
        stream.Flush();
        return counter;
      }
    }

    internal void CommitLevel(byte[] name, byte level, params ulong[] items)
    {
      lock (stream) {
        if (items.Length <= 0) {
          stream.Seek(0, SeekOrigin.End);
          stream.WriteByte(level);
          stream.WriteInt32(name.Length > 0 ? int.MinValue : 0);
          if (name.Length > 0) {
            stream.WriteInt32(name.Length);
            stream.Write(name);
          }

          stream.Flush();
          GetLevel(name).Remove(level);
          return;
        }

        items = items.OrderBy(i => i).Distinct().ToArray();
        stream.Seek(0, SeekOrigin.End);
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

    internal void Compact(Stream destination)
    {
      if (destination.Length > 0) {
        destination.Seek(0, SeekOrigin.Begin);
        destination.SetLength(0);
      }

      using var newManifest = new Manifest(location, destination, options, counter);
      foreach (var family in levels) {
        foreach (var level in family.Value.Where(level => level.Value.Length > 0)) {
          newManifest.CommitLevel(family.Key, level.Key, level.Value);
        }
      }
    }

    internal FileInfo FindFile(ulong id)
    {
      return FindFile($"{id:D4}");
    }

    internal FileInfo FindFile(string filename)
    {
      return FindFile(location, options, filename);
    }

    internal IEnumerable<ulong> Sequence(byte[] name)
    {
      return GetLevel(name).OrderBy(i => i.Key).SelectMany(i => i.Value.Reverse());
    }
    private IEnumerable<ulong> FullSequence()
    {
      return levels.SelectMany(i => i.Value).OrderBy(i => i.Key).SelectMany(i => i.Value.Reverse());
    }

    private void InitEmpty()
    {
      stream.Seek(0, SeekOrigin.Begin);
      stream.WriteInt32(Constants.MAGIC);
      stream.WriteUInt64(counter);
      var transformed = options.BlockTransformer.TransformBlock(Constants.MagicBytes);
      stream.WriteInt32(transformed.Length);
      stream.Write(transformed);
      stream.SetLength(stream.Position);
      stream.Flush();
    }
  }
}
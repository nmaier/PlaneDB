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

    private readonly SortedList<byte, ulong[]> levels = new SortedList<byte, ulong[]>();
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
        if (count <= 0) {
          levels.Remove((byte)level);
          continue;
        }

        var items = Enumerable.Range(0, count).Select(_ => stream.ReadUInt64()).OrderBy(i => i).ToArray();
        levels[(byte)level] = items;
      }
    }

    internal byte HighestLevel => levels.Keys.LastOrDefault();

    public bool IsEmpty => levels.Count <= 0;

    internal SortedList<byte, ulong[]> AllLevels =>
      new SortedList<byte, ulong[]>(levels.ToDictionary(l => l.Key, l => l.Value.ToArray()));

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

    public void AddToLevel(byte level, ulong id)
    {
      ulong[] items;
      if (!levels.TryGetValue(level, out var val)) {
        items = levels[level] = new[] { id };
      }
      else {
        items = levels[level] = val.Concat(new[] { id }).OrderBy(i => i).ToArray();
      }

      stream.Seek(0, SeekOrigin.End);
      stream.WriteByte(level);
      stream.WriteInt32(items.Length);
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
        var valid = Sequence().ToLookup(i => i);
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

    public bool TryGetLevelIds(byte level, out ulong[] ids)
    {
      if (!levels.TryGetValue(level, out var myIds)) {
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

    internal void CommitLevel(byte level, params ulong[] items)
    {
      lock (stream) {
        if (items.Length <= 0) {
          stream.Seek(0, SeekOrigin.End);
          stream.WriteByte(level);
          stream.WriteInt32(0);
          stream.Flush();
          levels.Remove(level);
          return;
        }

        items = items.OrderBy(i => i).Distinct().ToArray();
        stream.Seek(0, SeekOrigin.End);
        stream.WriteByte(level);
        stream.WriteInt32(items.Length);
        foreach (var item in items) {
          stream.WriteUInt64(item);
        }

        stream.Flush();
        levels[level] = items;
      }
    }

    internal void Compact(Stream destination)
    {
      if (destination.Length > 0) {
        destination.Seek(0, SeekOrigin.Begin);
        destination.SetLength(0);
      }

      using var newManifest = new Manifest(location, destination, options, counter);
      foreach (var level in levels.Where(level => level.Value.Length > 0)) {
        newManifest.CommitLevel(level.Key, level.Value);
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

    internal IEnumerable<ulong> Sequence()
    {
      return levels.OrderBy(i => i.Key).SelectMany(i => i.Value.Reverse());
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
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  internal sealed class Manifest : IDisposable
  {
    private readonly SortedList<byte, ulong[]> levels = new SortedList<byte, ulong[]>();
    private readonly PlaneDBOptions options;
    private readonly Stream stream;
    private ulong counter;

    internal Manifest(Stream stream, PlaneDBOptions options)
      : this(stream, options, 0)
    {
    }

    private Manifest(Stream stream, PlaneDBOptions options, ulong counter)
    {
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
      if (magic2Length < 0 || magic2Length > short.MaxValue) {
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
        var items = Enumerable.Range(0, count).Select(_ => stream.ReadUInt64()).OrderBy(i => i).ToArray();
        levels[(byte)level] = items;
      }
    }

    internal byte HighestLevel => levels.Keys.LastOrDefault();

    public bool IsEmpty => levels.Count <= 0;

    public void Dispose()
    {
      stream.Flush();
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

      using var newManifest = new Manifest(destination, options, counter);
      foreach (var level in levels.Where(level => level.Value.Length > 0)) {
        newManifest.CommitLevel(level.Key, level.Value);
      }
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
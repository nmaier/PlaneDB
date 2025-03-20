using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using NMaier.BlockStream;

namespace NMaier.PlaneDB;

internal sealed class SSTableBuilder : IWritableTable, IDisposable
{
  private readonly IPlaneByteArrayComparer comparer;
  private readonly SortedList<byte[], ValueEntry> dictionary;
  private readonly BlockWriteOnceStream writer;

  internal SSTableBuilder(Stream stream, ReadOnlySpan<byte> salt, PlaneOptions options)
  {
    _ = stream.Seek(0, SeekOrigin.Begin);
    stream.WriteInt32(Constants.MAGIC);
    stream.WriteInt32(SSTable.TABLE_VERSION);
    stream.Write(salt);

    writer = new BlockWriteOnceStream(stream, options.GetTransformerFor(salt));

    comparer = options.Comparer;
    dictionary = new SortedList<byte[], ValueEntry>(comparer);
  }

  public void Dispose()
  {
    var items = dictionary.ToArray();
    var filter = new BloomFilter(items.Length, 0.025);

    var indexEntries = new List<IndexEntry>();
    var off = 0L;
    foreach (var block in GenerateIndexBlocks()) {
      var firstKey = block[0].Key;
      var lastKey = block[^1].Key;
      var entry = new IndexEntry(firstKey, lastKey, writer.Position);
      indexEntries.Add(entry);
      writer.WriteInt32(block.Length);
      writer.WriteInt64(off);
      foreach (var (key, (length, _)) in block) {
        writer.WriteInt32(key.Length);
        var valueLength = length switch {
          Constants.TOMBSTONE => Constants.TOMBSTONE,
          > 0 and <= Constants.INLINED_SIZE => -(length << 4),
          _ => length
        };
        writer.WriteInt32(valueLength);
      }

      foreach (var (key, (length, bytes)) in block) {
        writer.Write(key);
        filter.Add(key);
        if (length > Constants.INLINED_SIZE) {
          off += length;

          continue;
        }

        if (bytes == null || length <= 0) {
          continue;
        }

        writer.Write(bytes);
      }
    }

    var headerOffset = writer.Position;

    var bloom = filter.ToArray();
    writer.WriteInt32(-bloom.Length);
    writer.Write(bloom);

    writer.WriteInt32(indexEntries.Count);

    for (var i = 0; i < indexEntries.Count; ++i) {
      var (_, last, offset) = indexEntries[i];
      var logicalOffset = (int)(headerOffset - offset);
      var key = last.AsSpan();
      if (i < indexEntries.Count - 1) {
        var (first, _, _) = indexEntries[i + 1];
        key = FindShortestKey(last, first);
      }

      writer.WriteInt32(logicalOffset);
      writer.WriteInt32(key.Length);
      writer.Write(key);
    }

    writer.WriteInt64(writer.Position - headerOffset);
    dictionary.Clear();
    writer.Flush(true);
    writer.Dispose();

    return;

    Span<byte> FindShortestKey(ReadOnlySpan<byte> curr, ReadOnlySpan<byte> next)
    {
      if (curr.IsEmpty) {
        return Array.Empty<byte>();
      }

      var min = Math.Min(curr.Length, next.Length);
      var diff = 0;
      while (diff < min && curr[diff] == next[diff]) {
        diff++;
      }

      if (diff >= min) {
        return curr.ToArray();
      }

      var b = curr[diff];
      if (b >= 0xff || b + 1 >= next[diff]) {
        return curr.ToArray();
      }

      var rv = curr[..(diff + 1)].ToArray();
      rv[^1]++;

      return comparer.Compare(curr.ToArray(), rv) > 0 ? curr.ToArray() : rv;
    }

    IEnumerable<KeyValuePair<byte[], ValueEntry>[]> GenerateIndexBlocks()
    {
      var rv = new List<KeyValuePair<byte[], ValueEntry>>();
      var count = sizeof(long) + sizeof(int);

      var itemsPerBlock = Math.Max(
        Constants.MIN_ENTRIES_PER_INDEX_BLOCK,
        Math.Min(
          Constants.MAX_ENTRIES_PER_INDEX_BLOCK,
          items.Length / Constants.MAX_ENTRIES_PER_INDEX_BLOCK));

      foreach (var kv in items) {
        var len = kv.Key.Length + (sizeof(int) * 2);
        if (kv.Value is { Value: not null, Length: > 0 }) {
          len += kv.Value.Length;
        }

        if (rv.Count > 0 &&
            (count + len > BlockStream.BlockStream.BLOCK_SIZE ||
             rv.Count >= itemsPerBlock)) {
          yield return rv.ToArray();
          count = sizeof(long) + sizeof(int);
          rv.Clear();
        }

        rv.Add(kv);
        count += len;
      }

      if (rv.Count > 0) {
        yield return rv.ToArray();
      }
    }
  }

  public void Flush()
  {
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    var valueLength = value.Length;
    switch (valueLength) {
      case 0:
        dictionary.Add(key.ToArray(), new ValueEntry(0, null));

        return;
      case <= Constants.INLINED_SIZE:
        dictionary.Add(key.ToArray(), new ValueEntry(valueLength, value.ToArray()));

        return;
      default:
        writer.Write(value);
        dictionary.Add(key.ToArray(), new ValueEntry(valueLength, null));

        break;
    }
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    dictionary.Add(key.ToArray(), new ValueEntry(Constants.TOMBSTONE, null));
  }

  private sealed record IndexEntry(byte[] FirstKey, byte[] LastKey, long BlockOffset);

  private sealed record ValueEntry(int Length, byte[]? Value);
}

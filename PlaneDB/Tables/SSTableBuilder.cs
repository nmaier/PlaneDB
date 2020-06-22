using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using NMaier.BlockStream;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseIndexFromEndExpression")]
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  internal sealed class SSTableBuilder : IWriteOnlyTable, IDisposable
  {
    private const int MAX_PER_INDEX_BLOCK = 256;
    private readonly IByteArrayComparer comparer;
    private readonly SortedList<byte[], byte[]?> dictionary;
    private readonly BlockWriteOnceStream writer;
    private readonly bool fullySync;

    internal SSTableBuilder(Stream stream, PlaneDBOptions options)
    {
      writer = new BlockWriteOnceStream(stream, options.BlockTransformer);
      comparer = options.Comparer;
      fullySync = options.MaxJournalActions < 0;
      dictionary = new SortedList<byte[], byte[]?>(comparer);
    }

    public void Dispose()
    {
      var items = dictionary.OrderBy(k => k.Key, comparer).ToArray();
      var filter = new BloomFilter(items.Length, 0.025);
      foreach (var kv in items) {
        filter.Add(kv.Key);

        var value = kv.Value;
        if (value == null || value.Length <= 9) {
          continue;
        }

        writer.Write(value);
      }

      writer.SkipToNextBlock();

      IEnumerable<KeyValuePair<byte[], byte[]?>[]> GenerateIndexBlocks()
      {
        var rv = new List<KeyValuePair<byte[], byte[]?>>();
        var count = sizeof(long) + sizeof(int);
        foreach (var kv in items) {
          var len = kv.Key.Length + sizeof(int) * 2;
          if (kv.Value?.Length <= 9) {
            len += kv.Value.Length;
          }

          if (count + len > BlockStream.BlockStream.BLOCK_SIZE || rv.Count >= MAX_PER_INDEX_BLOCK) {
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

      var indexEntries = new List<IndexEntry>();
      var off = 0L;
      foreach (var block in GenerateIndexBlocks()) {
        var firstKey = block[0].Key;
        var lastKey = block[block.Length - 1].Key;
        var entry = new IndexEntry(firstKey, lastKey, writer.Position);
        indexEntries.Add(entry);
        writer.WriteInt32(block.Length);
        writer.WriteInt64(off);
        foreach (var kv in block) {
          writer.WriteInt32(kv.Key.Length);
          if (kv.Value == null) {
            writer.WriteInt32(Constants.TOMBSTONE);
          }
          else if (kv.Value.Length > 0 && kv.Value.Length <= 9) {
            var vl = -(kv.Value.Length << 4);
            writer.WriteInt32(vl);
          }
          else {
            writer.WriteInt32(kv.Value.Length);
            off += kv.Value.Length;
          }
        }

        foreach (var kv in block) {
          writer.Write(kv.Key);
          if (kv.Value?.Length > 0 && kv.Value?.Length <= 9) {
            writer.Write(kv.Value);
          }
        }

        // Try not to fragment too much
        if (writer.Position % BlockStream.BlockStream.BLOCK_SIZE > BlockStream.BlockStream.BLOCK_SIZE * 2 / 3) {
          writer.SkipToNextBlock();
        }
      }

      var headerOffset = writer.Position;

      var bloom = filter.ToArray();
      writer.WriteInt32(bloom.Length);
      writer.Write(bloom);

      writer.WriteInt32(indexEntries.Count);

      Span<byte> FindShortestKey(ReadOnlySpan<byte> curr, ReadOnlySpan<byte> next)
      {
        if (curr.IsEmpty) {
          return curr.ToArray();
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

        var rv = curr.Slice(0, diff + 1).ToArray();
        rv[rv.Length - 1]++;
        return comparer.Compare(curr.ToArray(), rv) > 0 ? curr.ToArray() : rv;
      }

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
      writer.Flush(fullySync);
      writer.Dispose();
    }

    public void Flush()
    {
      throw new NotSupportedException();
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      dictionary[key.ToArray()] = value.ToArray();
    }

    public bool Remove(ReadOnlySpan<byte> key)
    {
      dictionary[key.ToArray()] = null;
      return true;
    }

    public bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      dictionary[key.ToArray()] = value.ToArray();
      return true;
    }

    private readonly struct IndexEntry
    {
      private readonly byte[] firstKey;
      private readonly byte[] lastKey;
      private readonly long blockOffset;

      public IndexEntry(byte[] firstKey, byte[] lastKey, long blockOffset)
      {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.blockOffset = blockOffset;
      }

      internal void Deconstruct(out byte[] first, out byte[] last, out long offset)
      {
        first = firstKey;
        last = lastKey;
        offset = blockOffset;
      }
    }
  }
}
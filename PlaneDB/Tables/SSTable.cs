using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using NMaier.BlockStream;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  [SuppressMessage("ReSharper", "UseIndexFromEndExpression")]
  internal sealed class SSTable : IReadOnlyTable, IDisposable
  {
    private readonly IByteArrayComparer comparer;
    private readonly Lazy<Index> index;
    private readonly BlockReadOnlyStream reader;
    private readonly Stream stream;
    private long refs = 1;

    internal SSTable(Stream stream, IBlockCache? cache, PlaneDBOptions options)
    {
      this.stream = stream;
      comparer = options.Comparer;
      reader = OpenReaderStream(cache, options);
      index = new Lazy<Index>(() => new Index(reader), LazyThreadSafetyMode.ExecutionAndPublication);
    }

    internal long DiskSize => stream.Length;
    internal long RealSize => reader.Length;
    internal long BloomBits => index.Value.Filter.Size;

    internal long IndexBlockCount => index.Value.Indexes.Length;

    public void Dispose()
    {
      if (Interlocked.Decrement(ref refs) > 0) {
        return;
      }

      reader.Dispose();
      stream.Dispose();
    }

    public bool ContainsKey(ReadOnlySpan<byte> key, out bool removed)
    {
      var keyBytes = key.ToArray();
      if (!ContainsNot(keyBytes)) {
        return IndexBlockContainsKey(Upper(keyBytes).Value, keyBytes, out removed);
      }

      removed = false;
      return false;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
    {
      using var cursor = reader.CreateCursor();
      foreach (var kv in index.Value.Indexes) {
        cursor.Seek(kv.Value, SeekOrigin.Begin);
        var items = cursor.ReadInt32();
        var off = cursor.ReadInt64();
        var blengths = new byte[items * sizeof(int) * 2];
        cursor.ReadFullBlock(blengths);
        var loff = 0;
        for (; items > 0; --items) {
          var lengths = blengths.AsSpan(loff);
          var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths.Slice(sizeof(int)));
          loff += sizeof(int) * 2;
          var key = cursor.ReadFullBlock(klen);
          switch (vlen) {
            case Constants.TOMBSTONE:
              yield return new KeyValuePair<byte[], byte[]?>(key, null);
              break;
            case 0:
              yield return new KeyValuePair<byte[], byte[]?>(key, Array.Empty<byte>());
              break;
            default:
              if (vlen <= -16) {
                var vlen2 = -(vlen >> 4);
                var value = cursor.ReadFullBlock(vlen2);
                yield return new KeyValuePair<byte[], byte[]?>(key, value);
              }
              else {
                var old = cursor.Position;
                cursor.Seek(off, SeekOrigin.Begin);
                var value = new byte[vlen];
                cursor.ReadFullBlock(value);
                cursor.Seek(old, SeekOrigin.Begin);
                off += vlen;
                yield return new KeyValuePair<byte[], byte[]?>(key, value);
              }

              break;
          }
        }
      }
    }

    public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
    {
      using var cursor = reader.CreateCursor();
      foreach (var kv in index.Value.Indexes) {
        cursor.Seek(kv.Value, SeekOrigin.Begin);
        var items = cursor.ReadInt32();
        cursor.ReadInt64();
        var blengths = new byte[items * sizeof(int) * 2];
        cursor.ReadFullBlock(blengths);
        var loff = 0;
        for (; items > 0; --items) {
          var lengths = blengths.AsSpan(loff);
          var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths.Slice(sizeof(int)));
          loff += sizeof(int) * 2;
          var key = cursor.ReadFullBlock(klen);
          switch (vlen) {
            case Constants.TOMBSTONE:
              yield return new KeyValuePair<byte[], byte[]?>(key, null);
              break;
            case 0:
              yield return new KeyValuePair<byte[], byte[]?>(key, Array.Empty<byte>());
              break;
            default:
              if (vlen <= -16) {
                var vlen2 = -(vlen >> 4);
                cursor.Seek(vlen2, SeekOrigin.Current);
              }

              yield return new KeyValuePair<byte[], byte[]?>(key, Array.Empty<byte>());

              break;
          }
        }
      }
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
      var keyBytes = key.ToArray();
      if (!ContainsNot(key.ToArray())) {
        return TryGetFromIndexBlock(Upper(keyBytes).Value, keyBytes, out value);
      }

      value = null;
      return false;
    }

    internal void AddRef()
    {
      Interlocked.Increment(ref refs);
    }

    internal void Ensure()
    {
      var val = index.Value;
      if (val != index.Value) {
        throw new InvalidOperationException();
      }
    }

    private bool ContainsNot(byte[] key)
    {
      var idx = index.Value;
      return comparer.Compare(key, idx.FirstKey) < 0 || comparer.Compare(key, idx.LastKey) > 0 ||
             !idx.Filter.Contains(key);
    }

    private bool IndexBlockContainsKey(long offset, byte[] keyBytes, out bool removed)
    {
      using var cursor = reader.CreateCursor();
      cursor.Seek(offset, SeekOrigin.Begin);
      var items = cursor.ReadInt32();
      cursor.ReadInt64();
      var lengths = cursor.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
      for (; items > 0; --items) {
        var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        lengths = lengths.Slice(sizeof(int));
        var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        lengths = lengths.Slice(sizeof(int));
        var key = cursor.ReadFullBlock(klen);
        if (comparer.Equals(keyBytes, key)) {
          removed = vlen == Constants.TOMBSTONE;
          return true;
        }

        if (vlen > -16) {
          continue;
        }

        vlen = -(vlen >> 4);
        cursor.Seek(vlen, SeekOrigin.Current);
      }

      // not found
      removed = false;
      return false;
    }

    private BlockReadOnlyStream OpenReaderStream(IBlockCache? cache, PlaneDBOptions options)
    {
      return new BlockReadOnlyStream(stream, options.BlockTransformer, cache: cache);
    }

    private bool TryGetFromIndexBlock(long offset, byte[] keyBytes, out byte[]? value)
    {
      using var cursor = reader.CreateCursor();
      cursor.Seek(offset, SeekOrigin.Begin);
      var items = cursor.ReadInt32();
      var off = cursor.ReadInt64();
      var lengths = cursor.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
      for (; items > 0; --items) {
        var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        lengths = lengths.Slice(sizeof(int));
        var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        lengths = lengths.Slice(sizeof(int));
        var key = cursor.ReadFullBlock(klen);
        if (comparer.Equals(keyBytes, key)) {
          switch (vlen) {
            case Constants.TOMBSTONE:
              value = null;
              break;
            case 0:
              value = Array.Empty<byte>();
              break;
            default:
              if (vlen <= -16) {
                vlen = -(vlen >> 4);
                value = new byte[vlen];
                cursor.ReadFullBlock(value);
              }
              else {
                cursor.Seek(off, SeekOrigin.Begin);
                value = new byte[vlen];
                cursor.ReadFullBlock(value);
              }

              break;
          }

          return true;
        }

        if (vlen <= -16) {
          cursor.Seek(-(vlen >> 4), SeekOrigin.Current);
        }
        else if (vlen > 0) {
          off += vlen;
        }
      }

      // not found
      value = null;
      return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private KeyValuePair<byte[], long> Upper(byte[] keyBytes)
    {
      var idx = index.Value;
      if (idx.Indexes.Length < 8) {
        foreach (var kv in idx.Indexes) {
          if (comparer.Compare(keyBytes, kv.Key) <= 0) {
            return kv;
          }
        }
      }

      var lo = 0;
      var hi = idx.Indexes.Length;
      while (hi - lo > 0) {
        var half = (hi + lo) / 2;
        var middle = idx.Indexes[half];
        if (comparer.Compare(middle.Key, keyBytes) < 0) {
          lo = half + 1;
        }
        else {
          hi = half;
        }
      }

      return idx.Indexes[lo];
    }

    private sealed class Index
    {
      internal readonly BloomFilter Filter;
      internal readonly byte[] FirstKey;
      internal readonly KeyValuePair<byte[], long>[] Indexes;
      internal readonly byte[] LastKey;

      public Index(BlockReadOnlyStream stream)
      {
        using var reader = stream.CreateCursor();
        reader.Seek(-sizeof(long), SeekOrigin.End);
        var headerLen = reader.ReadInt64();

        var headerBytes = new byte[headerLen];
        var headerOffset = reader.Seek(-sizeof(long) - headerBytes.Length, SeekOrigin.End);
        reader.ReadFullBlock(headerBytes);

        using var header = new MemoryStream(headerBytes, false);

        var blen = header.ReadInt32();
        var binit = new byte[blen];
        header.ReadFullBlock(binit);
        Filter = new BloomFilter(binit);

        var len = header.ReadInt32();
        var idx = new List<KeyValuePair<byte[], long>>();
        for (var i = 0; i < len; ++i) {
          var logicalOffset = header.ReadInt32();
          var klen = header.ReadInt32();
          var key = new byte[klen];
          header.ReadFullBlock(key);
          var absOffset = headerOffset - logicalOffset;
          idx.Add(new KeyValuePair<byte[], long>(key, absOffset));
        }

        Indexes = idx.ToArray();
        {
          var firstIndex = Indexes.First();
          reader.Seek(firstIndex.Value, SeekOrigin.Begin);
          var items = reader.ReadInt32();
          reader.ReadInt64();
          var blengths = new byte[items * sizeof(int) * 2];
          reader.ReadFullBlock(blengths);
          var lengths = blengths.AsSpan();
          var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          FirstKey = reader.ReadFullBlock(klen);
        }
        LastKey = Indexes[Indexes.Length - 1].Key;
      }
    }
  }
}
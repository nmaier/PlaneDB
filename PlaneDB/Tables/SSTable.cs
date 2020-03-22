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
    private readonly BloomFilter filter;
    private readonly byte[] firstKey;
    private readonly KeyValuePair<byte[], long>[] indexes;
    private readonly byte[] lastKey;
    private readonly Stream reader;
    private readonly Stream stream;
    private long refs = 1;

    internal SSTable(Stream stream, IBlockCache? cache, PlaneDBOptions options)
    {
      this.stream = stream;
      comparer = options.Comparer;
      reader = new BlockReadOnlyStream(stream, options.BlockTransformer, cache: cache);

      reader.Seek(-sizeof(long), SeekOrigin.End);
      var headerLen = reader.ReadInt64();

      var headerBytes = new byte[headerLen];
      var headerOffset = reader.Seek(-sizeof(long) - headerBytes.Length, SeekOrigin.End);
      reader.ReadFullBlock(headerBytes);
      using var header = new MemoryStream(headerBytes, false);

      var blen = header.ReadInt32();
      var binit = new byte[blen];
      header.ReadFullBlock(binit);
      filter = new BloomFilter(binit);

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

      indexes = idx.ToArray();
      {
        var firstIndex = indexes.First();
        reader.Seek(firstIndex.Value, SeekOrigin.Begin);
        var items = reader.ReadInt32();
        reader.ReadInt64();
        var blengths = new byte[items * sizeof(int) * 2];
        reader.ReadFullBlock(blengths);
        var lengths = blengths.AsSpan();
        var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        firstKey = reader.ReadFullBlock(klen);
      }
      lastKey = indexes[indexes.Length - 1].Key;
    }

    internal long DiskSize => stream.Length;
    internal long RealSize => reader.Length;
    internal long BloomBits => filter.Size;

    internal long IndexBlockCount => indexes.Length;

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
      if (!ContainsNot(key.ToArray())) {
        return IndexBlockContainsKey(Upper(keyBytes).Value, keyBytes, out removed);
      }

      removed = false;
      return false;
    }

    public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
    {
      var taken = false;
      Monitor.Enter(reader, ref taken);
      try {
        foreach (var kv in indexes) {
          reader.Seek(kv.Value, SeekOrigin.Begin);
          var items = reader.ReadInt32();
          var off = reader.ReadInt64();
          var blengths = new byte[items * sizeof(int) * 2];
          reader.ReadFullBlock(blengths);
          var loff = 0;
          for (; items > 0; --items) {
            var lengths = blengths.AsSpan(loff);
            var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
            var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths.Slice(sizeof(int)));
            loff += sizeof(int) * 2;
            var key = reader.ReadFullBlock(klen);
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
                  var value = reader.ReadFullBlock(vlen2);
                  yield return new KeyValuePair<byte[], byte[]?>(key, value);
                }
                else {
                  var old = reader.Position;
                  reader.Seek(off, SeekOrigin.Begin);
                  var value = new byte[vlen];
                  reader.ReadFullBlock(value);
                  reader.Seek(old, SeekOrigin.Begin);
                  off += vlen;
                  yield return new KeyValuePair<byte[], byte[]?>(key, value);
                }

                break;
            }
          }
        }
      }
      finally {
        if (taken) {
          Monitor.Exit(reader);
        }
      }
    }

    public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
    {
      var taken = false;
      Monitor.Enter(reader, ref taken);
      try {
        foreach (var kv in indexes) {
          reader.Seek(kv.Value, SeekOrigin.Begin);
          var items = reader.ReadInt32();
          reader.ReadInt64();
          var blengths = new byte[items * sizeof(int) * 2];
          reader.ReadFullBlock(blengths);
          var loff = 0;
          for (; items > 0; --items) {
            var lengths = blengths.AsSpan(loff);
            var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
            var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths.Slice(sizeof(int)));
            loff += sizeof(int) * 2;
            var key = reader.ReadFullBlock(klen);
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
                  reader.Seek(vlen2, SeekOrigin.Current);
                }

                yield return new KeyValuePair<byte[], byte[]?>(key, Array.Empty<byte>());

                break;
            }

            if (vlen > 0) {
            }
          }
        }
      }
      finally {
        if (taken) {
          Monitor.Exit(reader);
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

    private bool ContainsNot(byte[] key)
    {
      return comparer.Compare(key, firstKey) < 0 || comparer.Compare(key, lastKey) > 0 || !filter.Contains(key);
    }

    private bool IndexBlockContainsKey(long offset, byte[] keyBytes, out bool removed)
    {
      var taken = false;
      Monitor.Enter(reader, ref taken);
      try {
        reader.Seek(offset, SeekOrigin.Begin);
        var items = reader.ReadInt32();
        reader.ReadInt64();
        var lengths = reader.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
        for (; items > 0; --items) {
          var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          lengths = lengths.Slice(sizeof(int));
          var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          lengths = lengths.Slice(sizeof(int));
          var key = reader.ReadFullBlock(klen);
          if (comparer.Equals(keyBytes, key)) {
            removed = vlen == Constants.TOMBSTONE;
            return true;
          }

          if (vlen > -16) {
            continue;
          }

          vlen = -(vlen >> 4);
          reader.Seek(vlen, SeekOrigin.Current);
        }

        // not found
        removed = false;
        return false;
      }
      finally {
        if (taken) {
          Monitor.Exit(reader);
        }
      }
    }

    private bool TryGetFromIndexBlock(long offset, byte[] keyBytes, out byte[]? value)
    {
      var taken = false;
      Monitor.Enter(reader, ref taken);
      try {
        reader.Seek(offset, SeekOrigin.Begin);
        var items = reader.ReadInt32();
        var off = reader.ReadInt64();
        var lengths = reader.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
        for (; items > 0; --items) {
          var klen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          lengths = lengths.Slice(sizeof(int));
          var vlen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
          lengths = lengths.Slice(sizeof(int));
          var key = reader.ReadFullBlock(klen);
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
                  reader.ReadFullBlock(value);
                }
                else {
                  reader.Seek(off, SeekOrigin.Begin);
                  value = new byte[vlen];
                  reader.ReadFullBlock(value);
                }

                break;
            }

            return true;
          }

          if (vlen <= -16) {
            reader.Seek(-(vlen >> 4), SeekOrigin.Current);
          }
          else if (vlen > 0) {
            off += vlen;
          }
        }

        // not found
        value = null;
        return false;
      }
      finally {
        if (taken) {
          Monitor.Exit(reader);
        }
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    KeyValuePair<byte[], long> Upper(byte[] keyBytes)
    {
      if (indexes.Length < 8) {
        foreach (var kv in indexes) {
          if (comparer.Compare(keyBytes, kv.Key) <= 0) {
            return kv;
          }
        }
      }

      var lo = 0;
      var hi = indexes.Length;
      while (hi - lo > 0) {
        var half = (hi + lo) / 2;
        var middle = indexes[half];
        if (comparer.Compare(middle.Key, keyBytes) < 0) {
          lo = half + 1;
        }
        else {
          hi = half;
        }
      }

      return indexes[lo];
    }
  }
}
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

using NMaier.BlockStream;

namespace NMaier.PlaneDB;

[DebuggerDisplay("SSTable({" + nameof(FileName) + "})")]
internal sealed class SSTable : ISSTable
{
  private const int ON_STACK_LIMIT = 512;
  internal const int TABLE_VERSION = 2;

  [MethodImpl(Constants.SHORT_METHOD)]
  private static byte[] KeyWithAdvance(
    byte[] key,
    int valLen,
    ReaderEnhancedStream cursor)
  {
    _ = cursor.Seek(-(valLen >> 4), SeekOrigin.Current);

    return key;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private static byte[] ReadFull(ReaderEnhancedStream cursor, ref long off, int valLen)
  {
    var old = cursor.Position;
    _ = cursor.Seek(off, SeekOrigin.Begin);
    var value = new byte[valLen];
    cursor.ReadFullBlock(value);
    _ = cursor.Seek(old, SeekOrigin.Begin);
    off += valLen;

    return value;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private static byte[] ReadInline(ReaderEnhancedStream cursor, int valLen)
  {
    return cursor.ReadFullBlock(-(valLen >> 4));
  }

  private readonly IBlockCache? cache;
  private readonly IPlaneByteArrayComparer comparer;
  private readonly Lazy<Index> index;
  private readonly BlockReadOnlyStream reader;
  private readonly Stream? stream;
  private bool deleteOnClose;
  private long refs = 1;

  internal SSTable(Stream stream, byte[]? salt, IBlockCache? cache, PlaneOptions options)
  {
    _ = stream.Seek(0, SeekOrigin.Begin);
    var magic = stream.ReadInt32();
    if (magic != Constants.MAGIC) {
      throw new PlaneDBBadMagicException();
    }

    var version = stream.ReadInt32();
    if (version != TABLE_VERSION) {
      throw new PlaneDBBadMagicException("Wrong table version");
    }

    var diskSalt = stream.ReadFullBlock(Constants.SALT_BYTES);
    if (salt != null && !salt.AsSpan().SequenceEqual(diskSalt)) {
      throw new PlaneDBBadMagicException("Bad salt");
    }

    this.stream = stream;
    this.cache = cache;
    comparer = options.Comparer;
    reader = new BlockReadOnlyStream(
      this.stream,
      options.GetTransformerFor(diskSalt),
      cache: this.cache);
    index = new Lazy<Index>(
      () => new Index(reader),
      LazyThreadSafetyMode.ExecutionAndPublication);
  }

  private string FileName => stream switch {
    FileStream fs => Path.GetFileName(fs.Name),
    _ => "(nil)"
  };

  public void Dispose()
  {
    switch (Interlocked.Decrement(ref refs)) {
      case 0: break;
      case < 0: throw new InvalidOperationException("Table references went negative");
      case > 0: return;
    }

    var fileName = stream is FileStream fileStream ? fileStream.Name : null;
    reader.Dispose();
    stream?.Dispose();
    cache?.Dispose();
    GC.SuppressFinalize(this);

    if (!deleteOnClose) {
      return;
    }

    if (string.IsNullOrEmpty(fileName)) {
      return;
    }

    try {
      File.Delete(fileName);
    }
    catch {
      // Will be garbage collected later
    }
  }

  public bool ContainsKey(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out bool removed)
  {
    var keyBytes = key.ToArray();
    if (!ContainsNot(keyBytes, hashes)) {
      return IndexBlockContainsKey(Upper(keyBytes).Offset, keyBytes, out removed);
    }

    removed = false;

    return false;
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
  {
    using var cursor = reader.CreateCursor();
    foreach (var kv in index.Value.Descriptors) {
      _ = cursor.Seek(kv.Offset, SeekOrigin.Begin);
      var items = cursor.ReadInt32();
      var off = cursor.ReadInt64();
      var lengthsBuffer = new byte[items * sizeof(int) * 2];
      cursor.ReadFullBlock(lengthsBuffer);
      var lengthsOffset = 0;
      for (; items > 0; --items) {
        var lengths = lengthsBuffer.AsSpan(lengthsOffset);
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        var valLen = BinaryPrimitives.ReadInt32LittleEndian(lengths[sizeof(int)..]);
        lengthsOffset += sizeof(int) * 2;
        var key = cursor.ReadFullBlock(keyLen);

        yield return valLen switch {
          Constants.TOMBSTONE => new KeyValuePair<byte[], byte[]?>(key, null),
          0 => new KeyValuePair<byte[], byte[]?>(key, []),
          <= -16 => new KeyValuePair<byte[], byte[]?>(key, ReadInline(cursor, valLen)),
          _ => new KeyValuePair<byte[], byte[]?>(key, ReadFull(cursor, ref off, valLen))
        };
      }
    }
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
  {
    using var cursor = reader.CreateCursor();
    foreach (var kv in index.Value.Descriptors) {
      _ = cursor.Seek(kv.Offset, SeekOrigin.Begin);
      var items = cursor.ReadInt32();
      _ = cursor.ReadInt64();
      var lengthsBuffer = new byte[items * sizeof(int) * 2];
      cursor.ReadFullBlock(lengthsBuffer);
      var lengthsOffset = 0;
      for (; items > 0; --items) {
        var lengths = lengthsBuffer.AsSpan(lengthsOffset);
        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
        var valLen = BinaryPrimitives.ReadInt32LittleEndian(lengths[sizeof(int)..]);
        lengthsOffset += sizeof(int) * 2;
        var key = cursor.ReadFullBlock(keyLen);

        yield return valLen switch {
          Constants.TOMBSTONE => new KeyValuePair<byte[], byte[]?>(key, null),
          <= -16 => new KeyValuePair<byte[], byte[]?>(
            KeyWithAdvance(key, valLen, cursor),
            []),
          _ => new KeyValuePair<byte[], byte[]?>(key, [])
        };
      }
    }
  }

  public bool TryGet(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out byte[]? value)
  {
    var keyBytes = key.ToArray();
    if (!ContainsNot(keyBytes, hashes)) {
      return TryGetFromIndexBlock(Upper(keyBytes).Offset, keyBytes, out value);
    }

    value = null;

    return false;
  }

  public void AddRef()
  {
    if (Interlocked.Increment(ref refs) <= 0) {
      throw new PlaneDBStateException("Tried to AddRef and already disposed SSTable");
    }
  }

  public long BloomBits => index.Value.Filter.Size;

  public void DeleteOnClose()
  {
    deleteOnClose = true;
  }

  public long DiskSize => stream?.Length ?? 0;

  public void EnsureLazyInit()
  {
    var val = index.Value;
    if (val != index.Value) {
      throw new InvalidOperationException();
    }
  }

  public long IndexBlockCount => index.Value.Descriptors.Length;
  public long RealSize => reader.Length;

  ~SSTable()
  {
    Dispose();
  }

  [Pure]
  private bool ContainsNot(in byte[] key, in BloomFilter.Hashes hashes)
  {
    var idx = index.Value;

    return comparer.Compare(key, idx.FirstKey) < 0 ||
           comparer.Compare(key, idx.LastKey) > 0 ||
           !idx.Filter.ContainsMaybe(hashes);
  }

  private bool IndexBlockContainsKey(long offset, byte[] keyBytes, out bool removed)
  {
    using var cursor = reader.CreateCursor();
    _ = cursor.Seek(offset, SeekOrigin.Begin);
    var items = cursor.ReadInt32();
    _ = cursor.ReadInt64();
    var lengths = cursor.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
    Span<byte> keyBuffer = stackalloc byte[ON_STACK_LIMIT];
    var keySpan = keyBytes.AsSpan();

    for (; items > 0; --items) {
      var keyLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
      lengths = lengths[sizeof(int)..];
      var valLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
      lengths = lengths[sizeof(int)..];
      if (keyBytes.Length == keyLen) {
        if (keyLen <= ON_STACK_LIMIT) {
          var key = keyBuffer[..keyLen];
          cursor.ReadFullBlock(key, keyLen);
          if (keySpan.SequenceEqual(key)) {
            removed = valLen == Constants.TOMBSTONE;

            return true;
          }
        }
        else {
          var key = cursor.ReadFullBlock(keyLen);
          if (keySpan.SequenceEqual(key)) {
            removed = valLen == Constants.TOMBSTONE;

            return true;
          }
        }
      }
      else {
        _ = cursor.Seek(keyLen, SeekOrigin.Current);
      }

      if (valLen > -16) {
        continue;
      }

      valLen = -(valLen >> 4);
      _ = cursor.Seek(valLen, SeekOrigin.Current);
    }

    // not found
    removed = false;

    return false;
  }

  private bool TryGetFromIndexBlock(long offset, byte[] keyBytes, out byte[]? value)
  {
    using var cursor = reader.CreateCursor();
    _ = cursor.Seek(offset, SeekOrigin.Begin);
    var items = cursor.ReadInt32();
    var off = cursor.ReadInt64();
    var lengths = cursor.ReadFullBlock(items * sizeof(int) * 2).AsSpan();
    Span<byte> keyBuffer = stackalloc byte[ON_STACK_LIMIT];
    var keySpan = keyBytes.AsSpan();

    for (; items > 0; --items) {
      var keyLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
      lengths = lengths[sizeof(int)..];
      var valLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);
      lengths = lengths[sizeof(int)..];
      bool same;
      if (keyBytes.Length == keyLen) {
        if (keyLen <= ON_STACK_LIMIT) {
          var key = keyBuffer[..keyLen];
          cursor.ReadFullBlock(key, keyLen);
          same = keySpan.SequenceEqual(key);
        }
        else {
          var key = cursor.ReadFullBlock(keyLen);
          same = keySpan.SequenceEqual(key);
        }
      }
      else {
        same = false;
        _ = cursor.Seek(keyLen, SeekOrigin.Current);
      }

      if (same) {
        switch (valLen) {
          case Constants.TOMBSTONE:
            value = null;

            break;
          case 0:
            value = [];

            break;
          default:
            if (valLen <= -16) {
              valLen = -(valLen >> 4);
            }
            else {
              _ = cursor.Seek(off, SeekOrigin.Begin);
            }

            value = new byte[valLen];
            cursor.ReadFullBlock(value);

            break;
        }

        return true;
      }

      switch (valLen) {
        case <= -16:
          _ = cursor.Seek(-(valLen >> 4), SeekOrigin.Current);

          break;
        case > 0:
          off += valLen;

          break;
      }
    }

    // not found
    value = null;

    return false;
  }

  [MethodImpl(Constants.HOT_METHOD)]
  private Index.IndexDescriptor Upper(byte[] keyBytes)
  {
    var idx = index.Value;
    if (idx.Descriptors.Length < 8) {
      foreach (var descriptor in idx.Descriptors) {
        if (comparer.Compare(keyBytes, descriptor.Key) <= 0) {
          return descriptor;
        }
      }
    }

    var lo = 0;
    var hi = idx.Descriptors.Length;
    while (hi - lo > 0) {
      var half = lo + ((hi - lo) / 2);
      var middle = idx.Descriptors[half];
      if (comparer.Compare(middle.Key, keyBytes) < 0) {
        lo = half + 1;
      }
      else {
        hi = half;
      }
    }

    return idx.Descriptors[lo];
  }

  private sealed class Index
  {
    private static BloomFilter ReadBloomFilter(Stream header)
    {
      var bloomLen = header.ReadInt32();
      var compact = false;
      if (bloomLen < 0) {
        bloomLen = -bloomLen;
        compact = true;
      }

      var bloomInit = new byte[bloomLen];
      header.ReadFullBlock(bloomInit);

      return new BloomFilter(bloomInit, compact);
    }

    private static MemoryStream ReadHeader(Stream reader, out long headerOffset)
    {
      _ = reader.Seek(-sizeof(long), SeekOrigin.End);
      var headerLen = reader.ReadInt64();

      // Actually read the header
      var headerBytes = new byte[headerLen];
      headerOffset = reader.Seek(-sizeof(long) - headerBytes.Length, SeekOrigin.End);
      reader.ReadFullBlock(headerBytes);

      return new MemoryStream(headerBytes, false);
    }

    private static IEnumerable<IndexDescriptor> ReadIndexDescriptors(
      Stream header,
      long headerOffset)
    {
      var len = header.ReadInt32();
      for (var i = 0; i < len; ++i) {
        var logicalOffset = header.ReadInt32();
        var keyLen = header.ReadInt32();
        var key = new byte[keyLen];
        header.ReadFullBlock(key);
        var absOffset = headerOffset - logicalOffset;

        yield return new IndexDescriptor(key, absOffset);
      }
    }

    internal readonly IndexDescriptor[] Descriptors;
    internal readonly BloomFilter Filter;
    internal readonly byte[] FirstKey;
    internal readonly byte[] LastKey;

    public Index(BlockReadOnlyStream stream)
    {
      if (stream.Length == 0) {
        throw new IOException("SSTable stream is empty");
      }

      using var reader = stream.CreateCursor();
      using var header = ReadHeader(reader, out var headerOffset);

      Filter = ReadBloomFilter(header);

      Descriptors = ReadIndexDescriptors(header, headerOffset).ToArray();

      DetermineKeyRange(reader, out FirstKey, out LastKey);
    }

    private void DetermineKeyRange(Stream reader, out byte[] firstKey, out byte[] lastKey)
    {
      if (Descriptors.Length == 0) {
        firstKey = lastKey = [];

        return;
      }

      var firstIndex = Descriptors[0];
      _ = reader.Seek(firstIndex.Offset, SeekOrigin.Begin);
      var items = reader.ReadInt32();
      _ = reader.ReadInt64();
      var lengthsBuffer = new byte[items * sizeof(int) * 2];
      reader.ReadFullBlock(lengthsBuffer);
      var lengths = lengthsBuffer.AsSpan();
      var firstLen = BinaryPrimitives.ReadInt32LittleEndian(lengths);

      firstKey = reader.ReadFullBlock(firstLen);
      lastKey = Descriptors[^1].Key;
    }

    internal readonly struct IndexDescriptor(byte[] key, long offset)
    {
      internal readonly byte[] Key = key;
      internal readonly long Offset = offset;
    }
  }
}

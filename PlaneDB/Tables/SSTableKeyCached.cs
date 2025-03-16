using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

using NMaier.BlockStream;

namespace NMaier.PlaneDB;

[DebuggerDisplay("SSTableCached({" + nameof(FileName) + "})")]
internal sealed class SSTableKeyCached : ISSTable
{
  private readonly IBlockCache? cache;
  private readonly SortedList<byte[], Entry> entries;
  private readonly BlockReadOnlyStream reader;
  private readonly Stream stream;
  private bool deleteOnClose;
  private long refs = 1;

  internal SSTableKeyCached(
    Stream stream,
    byte[]? salt,
    IBlockCache? cache,
    PlaneOptions options)
  {
    _ = stream.Seek(0, SeekOrigin.Begin);
    var magic = stream.ReadInt32();
    if (magic != Constants.MAGIC) {
      throw new PlaneDBBadMagicException();
    }

    var version = stream.ReadInt32();
    if (version != SSTable.TABLE_VERSION) {
      throw new PlaneDBBadMagicException("Wrong table version");
    }

    var diskSalt = stream.ReadFullBlock(Constants.SALT_BYTES);
    if (salt != null && !salt.AsSpan().SequenceEqual(diskSalt)) {
      throw new PlaneDBBadMagicException("Bad salt");
    }

    this.stream = stream;
    this.cache = cache;
    entries = new SortedList<byte[], Entry>(options.Comparer);
    reader = new BlockReadOnlyStream(
      this.stream,
      options.GetTransformerFor(diskSalt),
      cache: this.cache);
    var index = new Index(reader);
    BloomBits = index.FilterSize;
    IndexBlockCount = index.Offsets.Length;

    using var cursor = reader.CreateCursor();
    foreach (var offset in index.Offsets) {
      _ = cursor.Seek(offset, SeekOrigin.Begin);
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
        switch (valLen) {
          case Constants.TOMBSTONE:
            entries.Add(key, new Entry(Constants.TOMBSTONE, 0, null));

            break;
          case 0:
            entries.Add(key, new Entry(Constants.TOMBSTONE, 0, []));

            break;
          default:
            if (valLen <= -16) {
              var value = cursor.ReadFullBlock(-(valLen >> 4));
              entries.Add(key, new Entry(Constants.TOMBSTONE, 0, value));
            }
            else {
              entries.Add(key, new Entry(off, valLen, null));
              off += valLen;
            }

            break;
        }
      }
    }
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

    reader.Dispose();
    stream.Dispose();
    cache?.Dispose();
    GC.SuppressFinalize(this);

    if (!deleteOnClose) {
      return;
    }

    if (stream is not FileStream fileStream) {
      return;
    }

    try {
      File.Delete(fileStream.Name);
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
    if (entries.TryGetValue(key.ToArray(), out var entry)) {
      removed = entry is { Offset: Constants.TOMBSTONE, Value: null };

      return true;
    }

    removed = false;

    return false;
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
  {
    using var cursor = reader.CreateCursor();
    foreach (var (key, (offset, length, bytes)) in entries) {
      if (offset >= 0) {
        _ = cursor.Seek(offset, SeekOrigin.Begin);
        var value = new byte[length];
        cursor.ReadFullBlock(value);

        yield return new KeyValuePair<byte[], byte[]?>(key, value);
      }
      else {
        yield return new KeyValuePair<byte[], byte[]?>(key, bytes);
      }
    }
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
  {
    foreach (var (key, (offset, _, bytes)) in entries) {
      yield return offset >= 0
        ? new KeyValuePair<byte[], byte[]?>(key, [])
        : new KeyValuePair<byte[], byte[]?>(key, bytes);
    }
  }

  public bool TryGet(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out byte[]? value)
  {
    var keyBytes = key.ToArray();
    if (!entries.TryGetValue(keyBytes, out var entry)) {
      value = null;

      return false;
    }

    if (entry.Offset < 0) {
      value = entry.Value;

      return true;
    }

    using var cursor = reader.CreateCursor();
    _ = cursor.Seek(entry.Offset, SeekOrigin.Begin);
    value = new byte[entry.Length];
    cursor.ReadFullBlock(value);

    return true;
  }

  public void AddRef()
  {
    if (Interlocked.Increment(ref refs) <= 0) {
      throw new PlaneDBStateException(
        "Tried to AddRef and already disposed SSTableKeyCached");
    }
  }

  public long BloomBits { get; }

  public void DeleteOnClose()
  {
    deleteOnClose = true;
  }

  public long DiskSize => stream.Length;

  public void EnsureLazyInit()
  {
  }

  public long IndexBlockCount { get; }
  public long RealSize => reader.Length;

  ~SSTableKeyCached()
  {
    Dispose();
  }

  private sealed class Index
  {
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

    private static IEnumerable<long> ReadIndexDescriptors(
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

        yield return absOffset;
      }
    }

    internal readonly int FilterSize;
    internal readonly long[] Offsets;

    public Index(BlockReadOnlyStream stream)
    {
      if (stream.Length == 0) {
        throw new IOException("SSTable stream is empty");
      }

      using var reader = stream.CreateCursor();
      using var header = ReadHeader(reader, out var headerOffset);

      var descOffset = header.ReadInt32();
      if (descOffset < 0) {
        descOffset = -descOffset;
        FilterSize = descOffset / 8;
      }
      else {
        FilterSize = descOffset;
      }

      _ = header.Seek(descOffset, SeekOrigin.Current);

      Offsets = ReadIndexDescriptors(header, headerOffset).ToArray();
    }
  }

  private readonly struct Entry(long offset, int length, byte[]? value)
  {
    internal readonly long Offset = offset;
    internal readonly int Length = length;
    internal readonly byte[]? Value = value;

    public void Deconstruct(out long offset, out int length, out byte[]? value)
    {
      offset = Offset;
      length = Length;
      value = Value;
    }
  }
}

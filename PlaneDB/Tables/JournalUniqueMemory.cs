using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class JournalUniqueMemory : IJournal, IReadableTable
{
  private readonly List<KeyValuePair<byte[], byte[]?>> records = [];
  public int Count => records.Count;
  public bool IsEmpty => records.Count == 0;

  public void Dispose()
  {
    records.Clear();
  }

  public long JournalLength { get; private set; }

  public bool ContainsKey(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out bool removed)
  {
    throw new NotSupportedException();
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
  {
    return records;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
  {
    throw new NotSupportedException();
  }

  public bool TryGet(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out byte[]? value)
  {
    throw new NotSupportedException();
  }

  public void Flush()
  {
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    records.Add(new KeyValuePair<byte[], byte[]?>(key.ToArray(), value.ToArray()));
    JournalLength += key.Length + value.Length;
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    records.Add(new KeyValuePair<byte[], byte[]?>(key.ToArray(), null));
    JournalLength += key.Length;
  }

  internal void CopyTo(IWritableTable table)
  {
    foreach (var (key, value) in records) {
      if (value == null) {
        table.Remove(key);
      }
      else {
        table.Put(key, value);
      }
    }
  }
}

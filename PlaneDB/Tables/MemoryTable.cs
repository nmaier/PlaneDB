using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal sealed class MemoryTable : IMemoryTable
{
  private readonly IPlaneByteArrayComparer comparer;
  private bool copyOnWrite;
  private SortedList<byte[], byte[]?> dictionary;
  private BloomFilter filter = new(10240, 0.05);

  internal MemoryTable(PlaneOptions options, long generation)
  {
    comparer = options.Comparer;
    dictionary = new SortedList<byte[], byte[]?>(comparer);
    Generation = generation;
  }

  private MemoryTable(MemoryTable other)
  {
    comparer = other.comparer;
    dictionary = other.dictionary;
    Generation = other.Generation;
    copyOnWrite = true;
  }

  public IMemoryTable Clone()
  {
    copyOnWrite = true;

    return new MemoryTable(this);
  }

  public long Generation { get; private init; }

  public bool ContainsKey(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out bool removed)
  {
    removed = false;
    if (IsEmpty || ContainsNot(hashes)) {
      return false;
    }

    if (!dictionary.TryGetValue(key.ToArray(), out var val)) {
      return false;
    }

    removed = val == null;

    return true;
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
  {
    return dictionary;
  }

  public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
  {
    return Enumerate();
  }

  public bool TryGet(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out byte[]? value)
  {
    if (IsEmpty) {
      value = null;

      return false;
    }

    if (!ContainsNot(hashes) && dictionary.TryGetValue(key.ToArray(), out value)) {
      return true;
    }

    value = null;

    return false;
  }

  public long ApproxSize { get; private set; }
  public bool IsEmpty => dictionary.Count == 0;

  public void Flush()
  {
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    if (copyOnWrite) {
      Duplicate();
    }

    var keyBytes = key.ToArray();
    if (!ContainsNot(new BloomFilter.Hashes(key)) &&
        dictionary.TryGetValue(keyBytes, out var val)) {
      dictionary[keyBytes] = value.ToArray();
      ApproxSize += value.Length - val?.Length ?? 0;

      return;
    }

    dictionary[keyBytes] = value.ToArray();
    ApproxSize += key.Length + value.Length;
    filter.Add(key);
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    if (copyOnWrite) {
      Duplicate();
    }

    var keyArray = key.ToArray();
    if (!ContainsNot(new BloomFilter.Hashes(key)) &&
        dictionary.Remove(keyArray, out var val)) {
      if (val == null) {
        return;
      }

      ApproxSize -= val.Length;
    }

    dictionary[keyArray] = null; // tombstone;
    ApproxSize += key.Length;
    filter.Add(key);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  [System.Diagnostics.Contracts.Pure]
  private bool ContainsNot(in BloomFilter.Hashes hashes)
  {
    return !filter.ContainsMaybe(hashes);
  }

  private void Duplicate()
  {
    dictionary = new SortedList<byte[], byte[]?>(dictionary, comparer);
    var newFilter = new BloomFilter(10240, 0.05);
    newFilter.Seed(filter);
    filter = newFilter;
    copyOnWrite = false;
  }
}

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  [PublicAPI]
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  internal sealed class MemoryTable : IReadOnlyTable, IWriteOnlyTable
  {
    private readonly IByteArrayComparer comparer;
    private readonly SortedList<byte[], byte[]?> dictionary;
    private readonly BloomFilter filter = new BloomFilter(10240, 0.05);

    internal MemoryTable(PlaneDBOptions options)
    {
      comparer = options.Comparer;
      dictionary = new SortedList<byte[], byte[]?>(comparer);
    }

    internal long ApproxSize { get; private set; }

    public bool IsEmpty => dictionary.Count == 0;

    public bool ContainsKey(ReadOnlySpan<byte> key, out bool removed)
    {
      removed = false;
      if (IsEmpty || ContainsNot(key)) {
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
      return dictionary.OrderBy(i => i.Key, comparer);
    }

    public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
    {
      return Enumerate();
    }

    public bool TryGet(ReadOnlySpan<byte> key, out byte[]? value)
    {
      if (IsEmpty) {
        value = null;
        return false;
      }

      if (!ContainsNot(key) && dictionary.TryGetValue(key.ToArray(), out value)) {
        return true;
      }

      value = null;
      return false;
    }

    public void Flush()
    {
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      var keybytes = key.ToArray();
      if (!ContainsNot(key) && dictionary.TryGetValue(keybytes, out var val)) {
        ApproxSize -= val?.Length ?? 0;
        dictionary[keybytes] = value.ToArray();
        ApproxSize += value.Length;
        return;
      }

      dictionary[keybytes] = value.ToArray();
      ApproxSize += key.Length * 2 + value.Length;
      filter.Add(key);
    }

    public bool Remove(ReadOnlySpan<byte> key)
    {
      var keyArray = key.ToArray();
      if (!ContainsNot(key) && dictionary.Remove(keyArray, out var val)) {
        if (val == null) {
          return true;
        }

        ApproxSize -= val.Length;
      }

      dictionary[keyArray] = null; // tombstone;
      ApproxSize += key.Length * 2;
      filter.Add(key);
      return true;
    }

    public bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      var keyArray = key.ToArray();
      if (ContainsNot(key) || !dictionary.TryGetValue(keyArray, out var val)) {
        return false;
      }

      ApproxSize -= val?.Length ?? 0 - value.Length;
      dictionary[keyArray] = value.ToArray();
      return true;
    }

    public bool ContainsKey(ReadOnlySpan<byte> key)
    {
      if (IsEmpty || ContainsNot(key)) {
        return false;
      }

      return dictionary.ContainsKey(key.ToArray());
    }

    public void CopyTo(IWriteOnlyTable table)
    {
      foreach (var kv in dictionary.OrderBy(i => i.Key, comparer).ToArray()) {
        var key = kv.Key;
        var value = kv.Value;
        if (value == null) {
          table.Remove(key);
        }
        else {
          table.Put(key, value);
        }
      }
    }

    private bool ContainsNot(ReadOnlySpan<byte> key)
    {
      return !filter.Contains(key);
    }
  }
}
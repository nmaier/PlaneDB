using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace NMaier.PlaneDB
{
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  internal sealed class UniqueMemoryJournal : IJournal
  {
    private readonly List<KeyValuePair<byte[], byte[]?>> records = new List<KeyValuePair<byte[], byte[]?>>();

    public bool IsEmpty => records.Count == 0;

    public void Dispose()
    {
      records.Clear();
    }

    public long Length { get; private set; }

    public void Flush()
    {
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      records.Add(new KeyValuePair<byte[], byte[]?>(key.ToArray(), value.ToArray()));
      Length += key.Length + value.Length;
    }

    public bool Remove(ReadOnlySpan<byte> key)
    {
      records.Add(new KeyValuePair<byte[], byte[]?>(key.ToArray(), null));
      Length += key.Length;
      return true;
    }

    public bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      throw new NotSupportedException();
    }

    internal void CopyTo(IWriteOnlyTable table)
    {
      foreach (var kv in records) {
        var value = kv.Value;
        if (value == null) {
          table.Remove(kv.Key);
        }
        else {
          table.Put(kv.Key, value);
        }
      }
    }
  }
}
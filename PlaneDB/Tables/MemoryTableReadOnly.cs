using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class MemoryTableReadOnly : IMemoryTable
{
  public IMemoryTable Clone()
  {
    return this;
  }

  public long Generation => 0;

  [MethodImpl(Constants.SHORT_METHOD)]
  public bool ContainsKey(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out bool removed)
  {
    removed = false;

    return false;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate()
  {
    yield break;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys()
  {
    yield break;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryGet(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out byte[]? value)
  {
    value = null;

    return false;
  }

  public long ApproxSize => 0;
  public bool IsEmpty => true;

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Flush()
  {
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    throw new PlaneDBReadOnlyException();
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Remove(ReadOnlySpan<byte> key)
  {
    throw new PlaneDBReadOnlyException();
  }
}

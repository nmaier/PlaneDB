using System;
using System.Collections;
using System.Linq;
using System.Runtime.InteropServices;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal sealed class BloomFilter
{
  [System.Diagnostics.Contracts.Pure]
  private static int ComputeBestBits(int capacity, double errorRate)
  {
    return (int)Math.Ceiling(
      capacity * Math.Log(errorRate, 1.0 / Math.Pow(2, Math.Log(2.0))));
  }

  [System.Diagnostics.Contracts.Pure]
  private static int ComputeBestHashes(int capacity, double errorRate)
  {
    return (int)Math.Round(
      Math.Log(2.0) * ComputeBestBits(capacity, errorRate) / capacity);
  }

  private readonly int numHashes;
  private BitArray hashBits;

  internal BloomFilter(int numItems, double errorRate)
  {
    var bits = ComputeBestBits(numItems, errorRate);
    bits = (((bits - 1) / 32) + 1) * 32;
    numHashes = Math.Min(ComputeBestHashes(numItems, errorRate), byte.MaxValue);
    hashBits = new BitArray(bits);
  }

  internal BloomFilter(byte[] init, bool compact)
  {
    if (compact) {
      var integers = MemoryMarshal.Cast<byte, int>(init);
      numHashes = integers[0];
      hashBits = new BitArray(integers[1..].ToArray());
    }
    else {
      numHashes = init[0];
      hashBits = new BitArray(init.Skip(1).Select(i => i == 1).ToArray());
    }
  }

  [System.Diagnostics.Contracts.Pure]
  internal long Size => hashBits.Count;

  internal void Add(ReadOnlySpan<byte> item)
  {
    var (primaryHash, secondaryHash) = new Hashes(item);
    for (var i = 0; i < numHashes; i++) {
      var resultingHash = (primaryHash + (i * secondaryHash)) % hashBits.Count;
      hashBits[resultingHash < 0 ? -resultingHash : resultingHash] = true;
    }
  }

  [System.Diagnostics.Contracts.Pure]
  public bool ContainsMaybe(in Hashes hashes)
  {
    var (primaryHash, secondaryHash) = hashes;
    for (var i = 0; i < numHashes; i++) {
      var resultingHash = (primaryHash + (i * secondaryHash)) % hashBits.Count;
      if (!hashBits[resultingHash < 0 ? -resultingHash : resultingHash]) {
        return false;
      }
    }

    return true;
  }

  internal void Seed(BloomFilter other)
  {
    hashBits = new BitArray(other.hashBits);
  }

  internal byte[] ToArray()
  {
    var length = ((hashBits.Length - 1) / 32) + 2;
    var rv = new byte[length * sizeof(int)];
    var integers = new int[length - 1];
    var results = MemoryMarshal.Cast<byte, int>(rv);
    results[0] = numHashes;
    hashBits.CopyTo(integers, 0);
    integers.AsSpan().CopyTo(results[1..]);

    return rv;
  }

  internal readonly ref struct Hashes
  {
    private readonly int primary;
    private readonly int secondary;

    internal Hashes(in ReadOnlySpan<byte> key)
    {
      primary = key.ComputeXXHash(293);
      secondary = key.ComputeXXHash(697);
    }

    public void Deconstruct(out int primaryHash, out int secondaryHash)
    {
      primaryHash = primary;
      secondaryHash = secondary;
    }
  }
}

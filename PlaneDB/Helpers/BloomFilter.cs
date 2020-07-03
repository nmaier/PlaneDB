using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  [PublicAPI]
  internal sealed class BloomFilter
  {
    private static int ComputeBestBits(int capacity, double errorRate)
    {
      return (int)Math.Ceiling(capacity * Math.Log(errorRate, 1.0 / Math.Pow(2, Math.Log(2.0))));
    }

    private static int ComputeBestHashes(int capacity, double errorRate)
    {
      return (int)Math.Round(Math.Log(2.0) * ComputeBestBits(capacity, errorRate) / capacity);
    }

    private static int HashPrimary(ReadOnlySpan<byte> bytes)
    {
      return bytes.ComputeXXHash(293);
    }

    private static int HashSecondary(ReadOnlySpan<byte> bytes)
    {
      return bytes.ComputeXXHash(697);
    }

    private readonly BitArray hashBits;
    private readonly int numHashes;

    internal BloomFilter(int numHashes, int bits = 1024)
    {
      this.numHashes = numHashes;
      hashBits = new BitArray(bits);
    }

    internal BloomFilter(int numItems, double errorRate)
    {
      var bits = ComputeBestBits(numItems, errorRate);
      numHashes = Math.Min(ComputeBestHashes(numItems, errorRate), byte.MaxValue);
      hashBits = new BitArray(bits);
    }

    internal BloomFilter(IReadOnlyList<byte> init)
    {
      numHashes = init[0];
      hashBits = new BitArray(init.Skip(1).Select(i => i == 1).ToArray());
    }

    internal long Size => hashBits.Count;

    public bool Contains(ReadOnlySpan<byte> item)
    {
      var primaryHash = HashPrimary(item);
      var secondaryHash = HashSecondary(item);
      for (var i = 0; i < numHashes; i++) {
        var hash = ComputeHash(primaryHash, secondaryHash, i);
        if (!hashBits[hash]) {
          return false;
        }
      }

      return true;
    }

    internal void Add(ReadOnlySpan<byte> item)
    {
      var primaryHash = HashPrimary(item);
      var secondaryHash = HashSecondary(item);
      for (var i = 0; i < numHashes; i++) {
        var hash = ComputeHash(primaryHash, secondaryHash, i);
        hashBits[hash] = true;
      }
    }

    internal byte[] ToArray()
    {
      var rv = new byte[1 + hashBits.Length];
      rv[0] = (byte)numHashes;
      for (var i = 0; i < hashBits.Length; ++i) {
        if (hashBits[i]) {
          rv[i + 1] = 1;
        }
      }

      return rv;
    }

    private int ComputeHash(int primaryHash, int secondaryHash, int i)
    {
      var resultingHash = (primaryHash + i * secondaryHash) % hashBits.Count;
      return resultingHash < 0 ? -resultingHash : resultingHash;
    }
  }
}
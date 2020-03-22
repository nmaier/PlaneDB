using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <summary>
  ///   Your friendly byte array comparer
  /// </summary>
  /// <remarks>Lexical</remarks>
  [PublicAPI]
  public sealed class ByteArrayComparer : IByteArrayComparer, IComparer, IEqualityComparer
  {
    private const uint SEED = 293;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(object? x, object? y)
    {
      return Compare(x as byte[], y as byte[]);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(byte[]? x, byte[]? y)
    {
      if (x == null || y == null) {
#pragma warning disable CS8604 // Possible null reference argument.
        return Comparer<object>.Default.Compare(x, y);
#pragma warning restore CS8604 // Possible null reference argument.
      }

      return x.AsSpan().SequenceCompareTo(y);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    bool IEqualityComparer.Equals(object? x, object? y)
    {
      return Equals(x as byte[], y as byte[]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    int IEqualityComparer.GetHashCode(object obj)
    {
      return GetHashCode((byte[])obj);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Equals(byte[]? x, byte[]? y)
    {
      return x.AsSpan().SequenceEqual(y);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetHashCode(byte[] obj)
    {
      return Extensions.ComputeXXHash(obj, SEED);
    }
  }
}
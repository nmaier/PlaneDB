using System;
using System.Collections;
using System.Runtime.CompilerServices;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Your friendly byte array comparer
/// </summary>
/// <remarks>Lexical</remarks>
[PublicAPI]
public sealed class PlaneByteArrayComparer : IPlaneByteArrayComparer, IComparer,
  IEqualityComparer
{
  private const uint SEED = 293;

  /// <summary>
  ///   Default byte comparer
  /// </summary>
  public static readonly IPlaneByteArrayComparer Default = new PlaneByteArrayComparer();

  /// <inheritdoc />
  [System.Diagnostics.Contracts.Pure]
  [MethodImpl(Constants.SHORT_METHOD)]
  public int Compare(object? x, object? y)
  {
    return Compare(x as byte[], y as byte[]);
  }

  /// <inheritdoc />
  [System.Diagnostics.Contracts.Pure]
  [MethodImpl(Constants.SHORT_METHOD)]
  public int Compare(byte[]? x, byte[]? y)
  {
    return x.AsSpan().SequenceCompareTo(y);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  [System.Diagnostics.Contracts.Pure]
  bool IEqualityComparer.Equals(object? x, object? y)
  {
    return Equals(x as byte[], y as byte[]);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  [System.Diagnostics.Contracts.Pure]
  int IEqualityComparer.GetHashCode(object obj)
  {
    return GetHashCode((byte[])obj);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  [System.Diagnostics.Contracts.Pure]
  public bool Equals(byte[]? x, byte[]? y)
  {
    return x.AsSpan().SequenceEqual(y);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  [System.Diagnostics.Contracts.Pure]
  public int GetHashCode(byte[] obj)
  {
    return InternalExtensions.ComputeXXHash(obj, SEED);
  }
}

using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace NMaier.PlaneDB;

internal sealed class KeyComparer : IComparer<KeyValuePair<byte[], byte[]?>>
{
  private readonly IPlaneByteArrayComparer cmp;

  internal KeyComparer(IPlaneByteArrayComparer comparer)
  {
    cmp = comparer;
  }

  [Pure]
  public int Compare(KeyValuePair<byte[], byte[]?> x, KeyValuePair<byte[], byte[]?> y)
  {
    return cmp.Compare(x.Key, y.Key);
  }
}

using System.Collections.Generic;

namespace NMaier.PlaneDB.Tests;

internal sealed class
  KVComparer<TKey, TValue> : IEqualityComparer<KeyValuePair<TKey, TValue>>
  where TKey : notnull
{
  private readonly IEqualityComparer<TKey> keyComparer;
  private readonly IEqualityComparer<TValue> valueComparer;

  internal KVComparer(
    IEqualityComparer<TKey> keyComparer,
    IEqualityComparer<TValue> valueComparer)
  {
    this.keyComparer = keyComparer;
    this.valueComparer = valueComparer;
  }

  public bool Equals(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
  {
    return keyComparer.Equals(x.Key, y.Key) && valueComparer.Equals(x.Value, y.Value);
  }

  public int GetHashCode(KeyValuePair<TKey, TValue> obj)
  {
    return keyComparer.GetHashCode(obj.Key) ^ valueComparer.GetHashCode(obj.Value!);
  }
}

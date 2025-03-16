using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

internal sealed class MergeEnumerator : IEnumerator<KeyValuePair<byte[], byte[]>>
{
  private readonly KeyComparer keyComparer;
  private readonly bool readValues;
  private readonly IReadableTable[] tables;
  private IEnumerator<KeyValuePair<byte[], byte[]?>> enumerator;

  internal MergeEnumerator(
    IReadableTable[] tables,
    bool readValues,
    IPlaneByteArrayComparer comparer)
  {
    this.tables = tables;
    this.readValues = readValues;
    keyComparer = new KeyComparer(comparer);
    foreach (var r in tables) {
      if (r is ISSTable t) {
        t.AddRef();
      }
    }

    enumerator = CreateInternal();
  }

  public void Dispose()
  {
    enumerator.Dispose();
    foreach (var disposable in tables.OfType<IDisposable>()) {
      disposable.Dispose();
    }
  }

  object IEnumerator.Current => Current;

  public bool MoveNext()
  {
    while (enumerator.MoveNext()) {
      if (enumerator.Current.Value == null) {
        continue;
      }

      return true;
    }

    return false;
  }

  public void Reset()
  {
    enumerator = CreateInternal();
  }

  public KeyValuePair<byte[], byte[]> Current =>
    new(enumerator.Current.Key, enumerator.Current.Value!);

  [MustDisposeResource]
  private IEnumerator<KeyValuePair<byte[], byte[]?>> CreateInternal()
  {
    return tables.Select(t => readValues ? t.Enumerate() : t.EnumerateKeys())
      .ToArray()
      .EnumerateSortedUniquely(keyComparer)
      .GetEnumerator();
  }
}

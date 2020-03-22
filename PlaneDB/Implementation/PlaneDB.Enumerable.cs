using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace NMaier.PlaneDB
{
  public sealed partial class PlaneDB
  {
    [SuppressMessage("ReSharper", "ConvertToUsingDeclaration")]
    private static IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateSortedTables(
      IEnumerable<KeyValuePair<byte[], byte[]?>>[] sequence, IByteArrayComparer comparer)
    {
      // ReSharper disable once ConvertIfStatementToSwitchStatement
      if (sequence.Length == 0) {
        return Array.Empty<KeyValuePair<byte[], byte[]?>>();
      }

      if (sequence.Length == 1) {
        return sequence[0];
      }

      if (sequence.Length == 2) {
        return MergeTwoSortedEnumerables(sequence[0], sequence[1], comparer);
      }

      var mid = (int)Math.Ceiling((double)sequence.Length / 2);
      var leftTables = sequence.AsSpan(0, mid).ToArray();
      var rightTables = sequence.AsSpan(mid).ToArray();
      return MergeTwoSortedEnumerables(EnumerateSortedTables(leftTables, comparer),
                                       EnumerateSortedTables(rightTables, comparer),
                                       comparer);
    }

    [SuppressMessage("ReSharper", "ConvertToUsingDeclaration")]
    private static IEnumerable<KeyValuePair<byte[], byte[]?>> MergeTwoSortedEnumerables(
      IEnumerable<KeyValuePair<byte[], byte[]?>> leftIter, IEnumerable<KeyValuePair<byte[], byte[]?>> rightIter,
      IByteArrayComparer comparer)
    {
      using (var leftEnum = leftIter.GetEnumerator())
      using (var rightEnum = rightIter.GetEnumerator()) {
        if (!rightEnum.MoveNext()) {
          while (leftEnum.MoveNext()) {
            yield return leftEnum.Current;
          }

          yield break;
        }

        if (!leftEnum.MoveNext()) {
          while (rightEnum.MoveNext()) {
            yield return rightEnum.Current;
          }

          yield break;
        }

        for (;;) {
          var l = leftEnum.Current;
          var r = rightEnum.Current;
          var rv = comparer.Compare(l.Key, r.Key);
          if (rv == 0) {
            yield return l;
            // Skip over current r
            if (!rightEnum.MoveNext()) {
              while (leftEnum.MoveNext()) {
                yield return leftEnum.Current;
              }

              yield break;
            }

            if (leftEnum.MoveNext()) {
              continue;
            }

            yield return r;
            while (rightEnum.MoveNext()) {
              yield return rightEnum.Current;
            }

            yield break;
          }

          if (rv < 0) {
            yield return l;
            if (leftEnum.MoveNext()) {
              continue;
            }

            yield return r;
            while (rightEnum.MoveNext()) {
              yield return rightEnum.Current;
            }

            yield break;
          }

          yield return r;
          if (rightEnum.MoveNext()) {
            continue;
          }

          yield return l;
          while (leftEnum.MoveNext()) {
            yield return leftEnum.Current;
          }

          yield break;
        }
      }
    }


    private IEnumerable<KeyValuePair<byte[], byte[]>> GetInternalEnumerable(bool readValues)
    {
      using var enumerator = GetInternalEnumerator(readValues);
      while (enumerator.MoveNext()) {
        yield return enumerator.Current;
      }
    }

    private Enumerator GetInternalEnumerator(bool readValues)
    {
      var t = new List<IReadOnlyTable>();
      if (!memoryTable.IsEmpty) {
        var mt = new MemoryTable(options);
        memoryTable.CopyTo(mt);
        t.Add(mt);
      }

      t.AddRange(tables.Select(i => i.Value));
      return new Enumerator(t.ToArray(), readValues, options.Comparer);
    }

    private sealed class Enumerator : IEnumerator<KeyValuePair<byte[], byte[]>>
    {
      private readonly IByteArrayComparer comparer;
      private readonly bool readValues;
      private readonly IReadOnlyTable[] tables;
      private IEnumerator<KeyValuePair<byte[], byte[]?>> enumerator;

      internal Enumerator(IReadOnlyTable[] tables, bool readValues, IByteArrayComparer comparer)
      {
        this.tables = tables;
        this.readValues = readValues;
        this.comparer = comparer;
        foreach (var r in tables) {
          if (r is SSTable t) {
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
        new KeyValuePair<byte[], byte[]>(enumerator.Current.Key,
                                         enumerator.Current.Value ?? throw new ArgumentNullException());

      private IEnumerator<KeyValuePair<byte[], byte[]?>> CreateInternal()
      {
        return EnumerateSortedTables(tables.Select(t => readValues ? t.Enumerate() : t.EnumerateKeys()).ToArray(),
                                     comparer).GetEnumerator();
      }
    }
  }
}
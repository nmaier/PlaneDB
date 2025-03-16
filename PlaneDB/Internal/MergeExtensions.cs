using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal static class MergeExtensions
{
  [MethodImpl(Constants.SHORT_METHOD)]
  internal static IEnumerable<T> EnumerateSortedUniquely<T>(
    this IEnumerable<T>[] sequence)
  {
    return EnumerateSortedUniquely(sequence, Comparer<T>.Default);
  }

  internal static IEnumerable<T> EnumerateSortedUniquely<T>(
    this IEnumerable<T>[] sequence,
    IComparer<T> comparer)
  {
    switch (sequence.Length) {
      case 0:
        return Array.Empty<T>();
      case 1:
        return sequence[0];
      case 2:
        return MergeTwoSortedEnumerables(sequence[0], sequence[1], comparer);
      default:
        var mid = (int)Math.Ceiling((double)sequence.Length / 2);
        var leftTables = sequence.AsSpan(0, mid).ToArray();
        var rightTables = sequence.AsSpan(mid).ToArray();

        return MergeTwoSortedEnumerables(
          EnumerateSortedUniquely(leftTables, comparer),
          EnumerateSortedUniquely(rightTables, comparer),
          comparer);
    }
  }

  private static IEnumerable<T> MergeTwoSortedEnumerables<T>(
    IEnumerable<T> leftIter,
    IEnumerable<T> rightIter,
    IComparer<T> comparer)
  {
    using var leftEnum = leftIter.GetEnumerator();
    using var rightEnum = rightIter.GetEnumerator();
    if (!rightEnum.MoveNext()) {
      while (leftEnum.MoveNext()) {
        yield return leftEnum.Current;
      }

      yield break;
    }

    if (!leftEnum.MoveNext()) {
      yield return rightEnum.Current;
      while (rightEnum.MoveNext()) {
        yield return rightEnum.Current;
      }

      yield break;
    }

    for (;;) {
      var l = leftEnum.Current;
      var r = rightEnum.Current;
      var rv = comparer.Compare(l, r);
      switch (rv) {
        case 0: {
          // Same, pick left
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

          yield return rightEnum.Current;
          while (rightEnum.MoveNext()) {
            yield return rightEnum.Current;
          }

          yield break;
        }
        case < 0: {
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
        default:
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
}

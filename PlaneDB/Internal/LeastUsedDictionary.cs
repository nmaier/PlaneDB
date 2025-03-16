using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

internal sealed class
  LeastUsedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
  where TKey : notnull where TValue : class
{
  public delegate bool RemoveIfPredicate(in TKey key);

  [Conditional("DEBUG")]
  private static void DebugIncrement(ref long val)
  {
    _ = Interlocked.Increment(ref val);
  }

  private readonly int capacity;
  private readonly ConcurrentDictionary<TKey, Node> items = new();
  private readonly Random random = new();
  private readonly ConcurrentDictionary<TKey, WeakReference<TValue>> secondary = new();
  private readonly int toDrop;
  private long count;
  private long hits;
  private long misses;
  private long shits;

  internal LeastUsedDictionary(int capacity)
  {
    this.capacity = capacity > 0
      ? capacity
      : throw new ArgumentException("Must be positive", nameof(capacity));
    toDrop = Math.Max(Math.Max(5, (int)(capacity * 0.1)), 1);
  }

  [MustDisposeResource]
  IEnumerator IEnumerable.GetEnumerator()
  {
    return GetEnumerator();
  }

  [MustDisposeResource]
  public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
  {
    return Iter().GetEnumerator();

    IEnumerable<KeyValuePair<TKey, TValue>> Iter()
    {
      foreach (var item in items.ToArray()) {
        yield return new KeyValuePair<TKey, TValue>(item.Key, item.Value.Obj);
      }
    }
  }

  public void RemoveIf(RemoveIfPredicate predicate)
  {
    var pairs = items.ToArray();
    foreach (var kv in pairs) {
      if (!predicate(kv.Key)) {
        continue;
      }

      _ = TryRemove(kv.Key);
    }
  }

  public void Set(TKey key, TValue value)
  {
    var balance = true;
    try {
      _ = items.AddOrUpdate(
        key,
        __ => {
          _ = Interlocked.Increment(ref count);

          return new Node(value);
        },
        (__, old) => {
          balance = false;
          if (!ReferenceEquals(old.Obj, value)) {
            return new Node(value) { Count = old.Count++ };
          }

          _ = Interlocked.Increment(ref old.Count);

          return old;
        });
    }
    finally {
      if (balance && count > capacity && Monitor.TryEnter(items)) {
        try {
          if (items.Count > capacity) {
            var rem = items.ToArray()
              .OrderBy(i => i.Value.Count)
              .ThenBy(_ => random.Next())
              .Select(i => i.Key)
              .Take(toDrop);
            foreach (var k in rem) {
              if (items.TryRemove(k, out var n)) {
                _ = secondary.TryAdd(k, new WeakReference<TValue>(n.Obj));
              }
            }

            foreach (var n in items.Values) {
              _ = Interlocked.Exchange(ref n.Count, 1);
            }

            _ = Interlocked.Exchange(ref count, items.Count);
          }
        }
        finally {
          Monitor.Exit(items);
        }
      }
    }
  }

  public bool TryGetValue(TKey key, out TValue? value)
  {
    if (items.TryGetValue(key, out var node)) {
      value = node.Obj;
      _ = Interlocked.Increment(ref node.Count);
      DebugIncrement(ref hits);

      return true;
    }

    if (secondary.TryRemove(key, out var r) && r.TryGetTarget(out value)) {
      Set(key, value);
      DebugIncrement(ref shits);

      return true;
    }

    DebugIncrement(ref misses);
    value = null;

    return false;
  }

  public bool TryRemove(TKey key)
  {
    return items.TryRemove(key, out _);
  }

  private sealed class Node
  {
    internal readonly TValue Obj;
    internal long Count;

    internal Node(TValue obj)
    {
      Obj = obj;
    }
  }
}

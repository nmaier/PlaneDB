using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;

namespace NMaier.PlaneDB
{
  internal sealed class LeastUsedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    where TValue : class
  {
    sealed class Node
    {
      internal readonly TValue Obj;
      internal long Count;

      internal Node(TValue obj)
      {
        Obj = obj;
      }
    }

    private readonly int capacity;
    private readonly ConcurrentDictionary<TKey, Node> items = new ConcurrentDictionary<TKey, Node>();
    private readonly ConcurrentDictionary<TKey, WeakReference<TValue>> secondary = new ConcurrentDictionary<TKey, WeakReference<TValue>>();
    private readonly int toDrop;
    private readonly Random random = new Random();
    private long count;
    private long hits;
    private long shits;
    private long misses;

    internal LeastUsedDictionary(int capacity)
    {
      if (capacity <= 0) {
        throw new ArgumentException("Must be positive", nameof(capacity));
      }

      this.capacity = capacity;
      toDrop = Math.Max(Math.Max(5, (int)(capacity * 0.1)), 1);
    }


    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    [SuppressMessage("ReSharper", "UseDeconstruction")]
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
      IEnumerable<KeyValuePair<TKey, TValue>> Iter()
      {
        foreach (var item in items.ToArray()) {
          yield return new KeyValuePair<TKey, TValue>(item.Key, item.Value.Obj);
        }
      }

      return Iter().GetEnumerator();
    }

    public void Remove(TKey key)
    {
      TryRemove(key);
    }

    public void Set(TKey key, TValue value)
    {
      var rebalance = true;
      try {
        items.AddOrUpdate(key, k => {
          Interlocked.Increment(ref count);
          return new Node(value);
        }, (k, old) => {
          rebalance = false;
          return new Node(value) { Count = old.Count++ };
        });
      }
      finally {
        if (rebalance && count > capacity && Monitor.TryEnter(items)) {
          try {
            if (items.Count > capacity) {
              var rem = items.ToArray().OrderBy(i => i.Value.Count).ThenBy(i => random.Next()).Select(i => i.Key)
                .Take(toDrop);
              foreach (var k in rem) {
                items.TryRemove(k, out var n);
                secondary.TryAdd(k, new WeakReference<TValue>(n.Obj));
              }

              foreach (var n in items.Values) {
                Interlocked.Exchange(ref n.Count, 1);
              }

              Interlocked.Exchange(ref count, items.Count);
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
        Interlocked.Increment(ref node.Count);
        Interlocked.Increment(ref hits);
        return true;
      }

      if (secondary.TryRemove(key, out var r) && r.TryGetTarget(out value)) {
        Set(key, value);
        Interlocked.Increment(ref shits);
        return true;
      }

      Interlocked.Increment(ref misses);
      value = default;
      return false;
    }

    public void TryRemove(TKey key)
    {
      items.TryRemove(key, out _);
    }
  }
}
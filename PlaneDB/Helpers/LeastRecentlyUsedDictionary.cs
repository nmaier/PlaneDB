using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace NMaier.PlaneDB
{
  internal sealed class LeastRecentlyUsedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    where TValue : class
  {
    private readonly int capacity;
    private readonly IDictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>> items;
    private readonly ReaderWriterLockSlim lockSlim = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
    private readonly LinkedList<KeyValuePair<TKey, TValue>> order = new LinkedList<KeyValuePair<TKey, TValue>>();
    private readonly int toDrop;

    internal LeastRecentlyUsedDictionary(int capacity)
    {
      if (capacity <= 0) {
        throw new ArgumentException("Must be positive", nameof(capacity));
      }

      this.capacity = capacity;
      toDrop = Math.Max(Math.Min(5, (int)(capacity * 0.03)), 1);

      items = new Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>>(capacity + toDrop);
    }


    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
      IEnumerable<KeyValuePair<TKey, TValue>> Iter()
      {
        foreach (var item in items.Values.ToArray()) {
          yield return new KeyValuePair<TKey, TValue>(item.Value.Key, item.Value.Value);
        }
      }

      lockSlim.EnterReadLock();
      try {
        return Iter().GetEnumerator();
      }
      finally {
        lockSlim.ExitReadLock();
      }
    }

    public void Remove(TKey key)
    {
      TryRemove(key, out _);
    }

    public void Set(TKey key, TValue value)
    {
      var rebalance = true;
      lockSlim.EnterWriteLock();
      try {
        if (items.TryGetValue(key, out var node)) {
          // we are replacing; no need to rebalance
          rebalance = false;
          order.Remove(node);
        }

        node = order.AddFirst(new KeyValuePair<TKey, TValue>(key, value));
        items[key] = node;
      }
      finally {
        if (rebalance) {
          MaybeDropSome();
        }

        lockSlim.ExitWriteLock();
      }
    }

    public bool TryGetValue(TKey key, out TValue? value)
    {
      lockSlim.EnterUpgradeableReadLock();
      try {
        if (!TryGetValueUpgradeable(key, out var val)) {
          value = default;
          return false;
        }

        value = val.Value.Value;
        return true;
      }
      finally {
        lockSlim.ExitUpgradeableReadLock();
      }
    }

    public bool TryRemove(TKey key, out TValue? value)
    {
      lockSlim.EnterUpgradeableReadLock();
      try {
        if (!items.TryGetValue(key, out var node)) {
          value = default;
          return false;
        }

        lockSlim.EnterWriteLock();
        try {
          value = node.Value.Value;
          order.Remove(node);
          items.Remove(key);
          return true;
        }
        finally {
          lockSlim.ExitWriteLock();
        }
      }
      finally {
        lockSlim.ExitUpgradeableReadLock();
      }
    }

    private void MaybeDropSome()
    {
      if (items.Count <= capacity) {
        return;
      }

      for (var i = 0; i < toDrop; ++i) {
        var item = order.Last;
        var pair = item.Value;
        var key = pair.Key;
        order.RemoveLast();
        items.Remove(key);
      }
    }

    private bool TryGetValueUpgradeable(TKey key, out LinkedListNode<KeyValuePair<TKey, TValue>> value)
    {
      if (!items.TryGetValue(key, out value)) {
        return false;
      }

      lockSlim.EnterWriteLock();
      try {
        order.Remove(value);
        order.AddFirst(value);
        return true;
      }
      finally {
        lockSlim.ExitWriteLock();
      }
    }
  }
}
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisList : RedisValue
{
  private static RedisKey NodeKey(byte[] key, in long id)
  {
    var k = new byte[key.Length + sizeof(long)];
    key.AsSpan().CopyTo(k);
    BinaryPrimitives.WriteInt64LittleEndian(k.AsSpan(key.Length), id);

    return new RedisKey(k, RedisKeyType.ListNode);
  }

  private static bool TryGetNode(
    Dictionary<long, RedisListNode> list,
    RedisServerClient client,
    byte[] key,
    long id,
    [MaybeNullWhen(false)] out RedisListNode node)
  {
    if (list.TryGetValue(id, out node)) {
      return true;
    }

    if (client.TryGetValue(NodeKey(key, id), out var val) && val is RedisListNode n) {
      node = n;

      return true;
    }

    node = null!;

    return false;
  }

  private long counter;
  private long first;
  private long last;

  public RedisList(long expires = -1) : base(RedisValueType.List, expires)
  {
    first = last = -1;
  }

  internal RedisList(ReadOnlySpan<byte> value, long expires = -1) : base(
    RedisValueType.List,
    expires)
  {
    counter = BinaryPrimitives.ReadInt64LittleEndian(value);
    Count = BinaryPrimitives.ReadInt64LittleEndian(value[sizeof(long)..]);
    first = BinaryPrimitives.ReadInt64LittleEndian(value[(sizeof(long) * 2)..]);
    last = BinaryPrimitives.ReadInt64LittleEndian(value[(sizeof(long) * 3)..]);
  }

  internal long Count { get; private set; }

  internal override byte[] Serialize()
  {
    var rv = new byte[(sizeof(long) * 5) + 1];
    rv[0] = (byte)RedisValueType.List;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + sizeof(long)), counter);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 2)), Count);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 3)), first);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 4)), last);

    return rv;
  }

  public override string ToString()
  {
    return $"List(c={Count},f={first},l={last})";
  }

  public void Clear(RedisServerClient client, in RedisKey key)
  {
    if (Count == 0) {
      return;
    }

    var keys = EnumerateReverse(client, key.KeyBytes, Count).ToArray();
    foreach (var k in keys) {
      if (k.Id >= 0) {
        _ = client.TryRemove(NodeKey(key.KeyBytes, k.Id), out _);
      }
    }
  }

  internal IEnumerable<RedisListNode> EnumerateForward(
    RedisServerClient db,
    byte[] key,
    long maxCount)
  {
    if (!db.TryGetValue(NodeKey(key, first), out var val) || val is not RedisListNode n) {
      yield break;
    }

    yield return n;
    --maxCount;

    while (maxCount > 0 && n.TryGetNext(db, key, out var nn)) {
      yield return nn;
      n = nn;
      --maxCount;
    }
  }

  internal IEnumerable<RedisListNode> EnumerateReverse(
    RedisServerClient db,
    byte[] key,
    long maxCount)
  {
    if (!db.TryGetValue(NodeKey(key, last), out var val) || val is not RedisListNode n) {
      yield break;
    }

    yield return n;
    --maxCount;

    while (maxCount > 0 && n.TryGetPrevious(db, key, out var nn)) {
      yield return nn;
      n = nn;
      --maxCount;
    }
  }

  internal IEnumerable<RedisListNode> PopBack(
    RedisServerClient client,
    byte[] key,
    in long count)
  {
    if (Count == 0) {
      return [];
    }

    var keys = EnumerateReverse(client, key, count + 1).ToArray();
    if (keys.Length > count) {
      var newLast = keys[^1];
      client.SetValue(
        NodeKey(key, newLast.Id),
        new RedisListNode(newLast.Id, newLast.Previous, -1, newLast.Value));
      last = newLast.Id;
      Count -= count;

      keys = keys.AsSpan(0, keys.Length - 1).ToArray();

      foreach (var n in keys) {
        _ = client.TryRemove(NodeKey(key, n.Id), out _);
      }

      return keys;
    }

    first = -1;
    last = -1;
    Count = 0;
    foreach (var n in keys) {
      _ = client.TryRemove(NodeKey(key, n.Id), out _);
    }

    return keys;
  }

  internal IEnumerable<RedisListNode> PopFront(
    RedisServerClient client,
    byte[] key,
    in long count)
  {
    if (Count == 0) {
      return [];
    }

    var keys = EnumerateForward(client, key, count + 1).ToArray();
    if (keys.Length > count) {
      var newFirst = keys[^1];
      client.SetValue(
        NodeKey(key, newFirst.Id),
        new RedisListNode(newFirst.Id, -1, newFirst.Next, newFirst.Value));
      first = newFirst.Id;
      Count -= count;
      keys = keys.AsSpan(0, keys.Length - 1).ToArray();

      foreach (var n in keys) {
        _ = client.TryRemove(NodeKey(key, n.Id), out _);
      }

      return keys;
    }

    first = -1;
    last = -1;
    Count = 0;
    foreach (var n in keys) {
      _ = client.TryRemove(NodeKey(key, n.Id), out _);
    }

    return keys;
  }

  internal void PushBack(RedisServerClient db, byte[] key, params byte[][] values)
  {
    var list = new Dictionary<long, RedisListNode>();

    foreach (var value in values) {
      if (first == -1) {
        var id = ++counter;
        var head = new RedisListNode(id, -1, -1, value);
        first = last = id;
        ++Count;
        list[first] = head;

        continue;
      }

      if (!TryGetNode(list, db, key, last, out var lastNode)) {
        throw new InvalidCastException();
      }

      {
        var prev = last;
        var id = ++counter;
        var node = new RedisListNode(id, prev, -1, value);
        last = id;
        ++Count;
        list[last] = node;
        list[prev] = new RedisListNode(
          lastNode.Id,
          lastNode.Previous,
          last,
          lastNode.Value);
      }
    }

    foreach (var (item, value) in list) {
      db.SetValue(NodeKey(key, item), value);
    }
  }

  internal void PushFront(RedisServerClient client, byte[] key, params byte[][] values)
  {
    var list = new Dictionary<long, RedisListNode>();

    foreach (var value in values) {
      if (first == -1) {
        var id = ++counter;
        var head = new RedisListNode(id, -1, -1, value);
        first = last = id;
        ++Count;
        list[first] = head;

        continue;
      }

      if (!TryGetNode(list, client, key, first, out var firstNode)) {
        throw new InvalidCastException();
      }

      {
        var id = ++counter;
        var node = new RedisListNode(id, -1, first, value);
        var next = first;
        first = id;
        ++Count;
        list[first] = node;
        list[next] = new RedisListNode(
          firstNode.Id,
          first,
          firstNode.Next,
          firstNode.Value);
      }
    }

    foreach (var (item, value) in list) {
      client.SetValue(NodeKey(key, item), value);
    }
  }

  public void Replace(RedisServerClient client, byte[] key, long offset, byte[] value)
  {
    if (offset < 0) {
      offset = Count + offset;
    }

    if (offset < 0 ||
        offset >= Count ||
        !TryGetValue(client, key, offset, out var node)) {
      throw new ArgumentOutOfRangeException(nameof(offset));
    }

    node = new RedisListNode(node.Id, node.Previous, node.Next, value);
    client.SetValue(NodeKey(key, node.Id), node);
  }

  internal bool TryGetValue(
    RedisServerClient client,
    byte[] key,
    long index,
    [MaybeNullWhen(false)] out RedisListNode node)
  {
    if (index < 0) {
      index = Count + index;
    }

    if (index >= Count || index < 0) {
      node = null!;

      return false;
    }

    if (index > Count / 2) {
      if (!client.TryGetValue(NodeKey(key, last), out var val) ||
          val is not RedisListNode n) {
        node = null!;

        return false;
      }

      if (index == Count - 1) {
        node = n;

        return true;
      }

      for (var remain = Count - index - 1; remain > 0; --remain) {
        if (n.TryGetPrevious(client, key, out var nn)) {
          n = nn;

          continue;
        }

        node = null!;

        return false;
      }

      node = n;
    }
    else {
      if (!client.TryGetValue(NodeKey(key, first), out var val) ||
          val is not RedisListNode n) {
        node = null!;

        return false;
      }

      if (index == 0) {
        node = n;

        return true;
      }

      for (var remain = 1; remain <= index; ++remain) {
        if (n.TryGetNext(client, key, out var nn)) {
          n = nn;

          continue;
        }

        node = null!;

        return false;
      }

      node = n;
    }

    return true;
  }
}

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisSet : RedisValue
{
  private const long INTERN_LIMIT = 524288;

  private static RedisKey NodeKey(in byte[] key, in byte[] value)
  {
    var k = new byte[key.Length + value.Length];
    key.AsSpan().CopyTo(k);
    value.AsSpan().CopyTo(k.AsSpan(key.Length));

    return new RedisKey(k, RedisKeyType.SetNode);
  }

  private readonly long generation;
  private readonly List<RedisString> members = [];
  private long size;

  internal RedisSet(RedisServerClient client) : base(RedisValueType.Set, -1)
  {
    var sentinel = (RedisSetNode)client.AddOrUpdate(
      new RedisKey(Array.Empty<byte>(), RedisKeyType.SetSentinel),
      () => new RedisSetNode(1),
      (in RedisValue value) => {
        var s = (RedisSetNode)value;

        return new RedisSetNode(s.Generation + 1);
      });
    generation = sentinel.Generation;
  }

  internal RedisSet(ReadOnlySpan<byte> value, long expires = -1) : base(
    RedisValueType.Set,
    expires)
  {
    Count = BinaryPrimitives.ReadInt64LittleEndian(value);
    generation = BinaryPrimitives.ReadInt64LittleEndian(value[sizeof(long)..]);
    size = BinaryPrimitives.ReadInt64LittleEndian(value[(sizeof(long) * 2)..]);
    var internedCount =
      BinaryPrimitives.ReadInt64LittleEndian(value[(sizeof(long) * 3)..]);
    var sp = value[(sizeof(long) * 4)..];
    for (; internedCount > 0; --internedCount) {
      var len = BinaryPrimitives.ReadInt32LittleEndian(sp);
      members.Add(new RedisString(sp.Slice(sizeof(int), len).ToArray()));
      sp = sp[(sizeof(int) + len)..];
    }
  }

  internal long Count { get; private set; }

  internal override byte[] Serialize()
  {
    var rv = new byte[(sizeof(long) * 5) +
                      1 +
                      members.Sum(m => m.Value.Length) +
                      (sizeof(int) * members.Count)];
    rv[0] = (byte)RedisValueType.Set;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + sizeof(long)), Count);
    BinaryPrimitives.WriteInt64LittleEndian(
      rv.AsSpan(1 + (sizeof(long) * 2)),
      generation);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 3)), size);
    BinaryPrimitives.WriteInt64LittleEndian(
      rv.AsSpan(1 + (sizeof(long) * 4)),
      members.Count);
    var sp = rv.AsSpan(1 + (sizeof(long) * 5));
    foreach (var redisString in members) {
      BinaryPrimitives.WriteInt32LittleEndian(sp, redisString.Value.Length);
      sp = sp[sizeof(int)..];
      redisString.Value.AsSpan().CopyTo(sp);
      sp = sp[redisString.Value.Length..];
    }

    return rv;
  }

  internal bool Add(RedisServerClient client, byte[] redisKey, byte[] value)
  {
    if (!client.TryAdd(NodeKey(redisKey, value), () => new RedisSetNode(generation))) {
      return false;
    }

    Count++;
    size += value.Length;
    if (size > INTERN_LIMIT && members.Count > 0) {
      members.Clear();
    }
    else {
      var redisString = new RedisString(value);
      members.Insert(~members.BinarySearch(redisString), redisString);
    }

    return true;
  }

  internal bool Contains(RedisServerClient client, byte[] redisKey, byte[] value)
  {
    return members.Count > 0
      ? members.BinarySearch(new RedisString(value)) >= 0
      : HasNode(client, redisKey, value);
  }

  public IEnumerable<RedisString> Enumerate(RedisServerClient client, byte[] redisKey)
  {
    if (members.Count > 0) {
      foreach (var s in members) {
        yield return s;
      }

      yield break;
    }

    var keyLength = redisKey.Length;
    var keys = client.KeysIterator.Where(
        k => k.KeyType == RedisKeyType.SetNode &&
             k.KeyBytes.Length >= keyLength &&
             k.KeyBytes.AsSpan(0, keyLength).SequenceEqual(redisKey))
      .ToArray();
    foreach (var key in keys) {
      if (!client.TryGetValue(new RedisKey(key.KeyBytes, key.KeyType), out var value) ||
          value is not RedisSetNode n ||
          n.Generation != generation) {
        continue;
      }

      yield return new RedisString(key.KeyBytes.AsSpan(keyLength).ToArray());
    }
  }

  private bool HasNode(RedisServerClient client, byte[] key, byte[] value)
  {
    var redisKey = NodeKey(key, value);
    if (!client.TryGetValue(redisKey, out var item) ||
        item is not RedisSetNode nextNode) {
      return false;
    }

    if (nextNode.Generation == generation) {
      return true;
    }

    _ = client.TryRemove(redisKey, out _);

    return false;
  }

  internal IEnumerable<RedisString> PeekRandom(
    RedisServerClient client,
    byte[] key,
    long count)
  {
    var intCount = checked((int)count);

    return members.Count > 0
      ? members.Shuffled().Take(intCount).ToArray()
      : Enumerate(client, key).Shuffled().Take(intCount).ToArray();
  }

  internal IEnumerable<RedisString> PopRandom(
    RedisServerClient client,
    byte[] key,
    long count)
  {
    var removed = (RedisString[])PeekRandom(client, key, count);
    if (removed.Length == Count) {
      members.Clear();
      Count = 0;
      size = 0;

      return removed;
    }

    foreach (var s in removed) {
      _ = Remove(client, key, s.Value);
    }

    return removed;
  }

  internal bool Remove(RedisServerClient client, byte[] redisKey, byte[] value)
  {
    if (!client.TryRemove(NodeKey(redisKey, value), out var n) ||
        n is not RedisSetNode node ||
        node.Generation != generation) {
      return false;
    }

    if (members.Count > 0) {
      members.RemoveAt(members.BinarySearch(new RedisString(value)));
    }

    Count--;
    size -= value.Length;
    if (Count > 0 && size <= INTERN_LIMIT && members.Count == 0) {
      members.AddRange([.. Enumerate(client, redisKey).OrderBy(k => k)]);
    }

    return true;
  }
}

using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisListNode : RedisValue
{
  internal readonly long Id;
  internal readonly long Next;
  internal readonly long Previous;
  internal readonly byte[] Value;

  public RedisListNode(long id, long previous, long next, byte[] value, long expires = -1)
    : base(RedisValueType.ListNode, expires)
  {
    Id = id;
    Previous = previous;
    Next = next;
    Value = value;
  }

  public RedisListNode(ReadOnlySpan<byte> value, long expires = -1) : base(
    RedisValueType.ListNode,
    expires)
  {
    Id = BinaryPrimitives.ReadInt64LittleEndian(value);
    Previous = BinaryPrimitives.ReadInt64LittleEndian(value[sizeof(long)..]);
    Next = BinaryPrimitives.ReadInt64LittleEndian(value[(sizeof(long) * 2)..]);
    Value = value[(sizeof(long) * 3)..].ToArray();
  }

  internal override byte[] Serialize()
  {
    var rv = new byte[(sizeof(long) * 4) + Value.Length + 1];
    rv[0] = (byte)RedisValueType.ListNode;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + sizeof(long)), Id);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 2)), Previous);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + (sizeof(long) * 3)), Next);
    Value.AsSpan().CopyTo(rv.AsSpan(1 + (sizeof(long) * 4)));

    return rv;
  }

  public bool TryGetNext(
    RedisServerClient client,
    byte[] key,
    [MaybeNullWhen(false)] out RedisListNode node)
  {
    var k = new byte[key.Length + sizeof(long)];
    key.AsSpan().CopyTo(k);
    BinaryPrimitives.WriteInt64LittleEndian(k.AsSpan(key.Length), Next);
    if (client.TryGetValue(new RedisKey(k, RedisKeyType.ListNode), out var val) &&
        val is RedisListNode n) {
      node = n;

      return true;
    }

    node = null!;

    return false;
  }

  public bool TryGetPrevious(
    RedisServerClient client,
    byte[] key,
    [MaybeNullWhen(false)] out RedisListNode node)
  {
    var k = new byte[key.Length + sizeof(long)];
    key.AsSpan().CopyTo(k);
    BinaryPrimitives.WriteInt64LittleEndian(k.AsSpan(key.Length), Previous);
    if (client.TryGetValue(new RedisKey(k, RedisKeyType.ListNode), out var val) &&
        val is RedisListNode n) {
      node = n;

      return true;
    }

    node = null!;

    return false;
  }
}

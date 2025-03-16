using System;
using System.Buffers.Binary;
using System.Runtime.Serialization;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisValueSerializer : IPlaneSerializer<RedisValue>
{
  public RedisValue Deserialize(ReadOnlySpan<byte> bytes)
  {
    var expires = BinaryPrimitives.ReadInt64LittleEndian(bytes[1..]);
    var type = bytes[0];
    bytes = bytes[(1 + sizeof(long))..];
    switch ((RedisValueType)type) {
      case RedisValueType.String:
        return new RedisString(bytes.ToArray(), expires);
      case RedisValueType.Integer:
        var val = BinaryPrimitives.ReadInt64LittleEndian(bytes);

        return new RedisInteger(val, expires);
      case RedisValueType.Null:
        return new RedisNull(expires);
      case RedisValueType.List:
        return new RedisList(bytes, expires);
      case RedisValueType.ListNode:
        return new RedisListNode(bytes, expires);
      case RedisValueType.Set:
        return new RedisSet(bytes, expires);
      case RedisValueType.SetNode:
        return new RedisSetNode(bytes, expires);
      default:
        throw new SerializationException();
    }
  }

  public byte[] Serialize(in RedisValue obj)
  {
    return obj.Serialize();
  }
}

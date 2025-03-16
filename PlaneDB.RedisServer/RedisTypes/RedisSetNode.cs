using System;
using System.Buffers.Binary;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisSetNode : RedisValue
{
  internal readonly long Generation;

  internal RedisSetNode(long generation) : base(RedisValueType.SetNode, -1)
  {
    Generation = generation;
  }

  internal RedisSetNode(ReadOnlySpan<byte> value, long expires = -1) : base(
    RedisValueType.SetNode,
    expires)
  {
    Generation = BinaryPrimitives.ReadInt64LittleEndian(value);
  }

  internal override byte[] Serialize()
  {
    var rv = new byte[1 + (sizeof(long) * 2)];
    rv[0] = (byte)RedisValueType.SetNode;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + sizeof(long)), Generation);

    return rv;
  }
}

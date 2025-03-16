using System;
using System.Buffers.Binary;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisInteger(long value, long expires = -1)
  : RedisValue(RedisValueType.Integer, expires)
{
  internal readonly long Value = value;

  internal override byte[] Serialize()
  {
    var rv = new byte[1 + (sizeof(long) * 2)];
    rv[0] = (byte)RedisValueType.Integer;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1 + sizeof(long)), Value);

    return rv;
  }
}

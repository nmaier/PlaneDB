using System;
using System.Buffers.Binary;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisNull(long expires = -1)
  : RedisValue(RedisValueType.Null, expires)
{
  internal override byte[] Serialize()
  {
    var rv = new byte[1 + sizeof(long)];
    rv[0] = (byte)RedisValueType.Null;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);

    return rv;
  }
}

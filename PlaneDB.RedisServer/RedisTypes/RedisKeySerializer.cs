using System;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisKeySerializer : IPlaneSerializer<RedisKey>
{
  public RedisKey Deserialize(ReadOnlySpan<byte> bytes)
  {
    return new RedisKey(bytes[1..], (RedisKeyType)bytes[0]);
  }

  public byte[] Serialize(in RedisKey obj)
  {
    var rv = new byte[obj.KeyBytes.Length + 1];
    rv[0] = (byte)obj.KeyType;
    obj.KeyBytes.AsSpan().CopyTo(rv.AsSpan(1));

    return rv;
  }
}

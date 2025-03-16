using System;
using System.Text;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisTypes;

[PublicAPI]
internal readonly struct RedisKey(
  ReadOnlySpan<byte> keyBytes,
  RedisKeyType keyType = RedisKeyType.Normal)
{
  internal readonly RedisKeyType KeyType = keyType;
  internal readonly byte[] KeyBytes = keyBytes.ToArray();

  public RedisKey(string keyName, RedisKeyType keyType = RedisKeyType.Normal) : this(
    Encoding.UTF8.GetBytes(keyName),
    keyType)
  {
  }

  public RedisKey(byte[] keyBytes, RedisKeyType keyType = RedisKeyType.Normal) : this(
    keyBytes.AsSpan(),
    keyType)
  {
  }
}

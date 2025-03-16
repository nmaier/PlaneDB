using System;
using System.Globalization;

using JetBrains.Annotations;

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisTypes;

[PublicAPI]
internal abstract class RedisValue(RedisValueType type, long expires)
{
  internal readonly RedisValueType Type = type;
  internal long Expires = expires;
  public bool Expired => Expires > 0 && Expires < DateTime.UtcNow.Ticks;

  public long ExpiresInMilliseconds
  {
    get
    {
      if (Expires == -1) {
        return -1;
      }

      var now = DateTime.UtcNow.Ticks;
      var remain = Expires - now;

      return remain <= 0 ? -2 : remain / TimeSpan.TicksPerMillisecond;
    }
  }

  public long ExpiresInSeconds
  {
    get
    {
      if (Expires == -1) {
        return -1;
      }

      var now = DateTime.UtcNow.Ticks;
      var remain = Expires - now;

      return remain <= 0 ? -2 : remain / TimeSpan.TicksPerSecond;
    }
  }

  internal bool IsNull => this is RedisNull;

  internal RespType StringValue => this switch {
    { IsNull: true } or { Expired: true } => RespNullString.Value,
    RedisInteger redisInteger => new RespBulkString(
      redisInteger.Value.ToString(CultureInfo.InvariantCulture)),
    RedisString redisString => new RespBulkString(redisString.Value),
    RedisListNode redisListNode => new RespBulkString(redisListNode.Value),
    _ => RespNullString.Value
  };

  internal abstract byte[] Serialize();
}

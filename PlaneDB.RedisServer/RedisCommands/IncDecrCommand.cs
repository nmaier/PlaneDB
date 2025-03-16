using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class IncDecrCommand : IRedisCommand
{
  internal static RespType Increment(RedisServerClient client, byte[] key, long val)
  {
    var rv = (RedisInteger)client.AddOrUpdate(
      new RedisKey(key),
      () => new RedisInteger(val),
      (in RedisValue value) => {
        var newValue = value switch {
          RedisInteger redisInteger => redisInteger.Value + val,
          RedisNull => val,
          RedisString redisString => redisString.IntValue + val,
          _ => throw new ArgumentOutOfRangeException(nameof(value))
        };

        return new RedisInteger(newValue, value.Expires);
      });

    return new RespInteger(rv.Value);
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return Increment(client, args[0].AsBytes(), cmd == "incr" ? 1 : -1);
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

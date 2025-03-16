using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class StrLenCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (!client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)) {
      return new RespInteger(0);
    }

    var str = val switch {
      RedisInteger redisInteger => redisInteger.StringValue.AsBytes(),
      RedisString redisString => redisString.Value,
      _ => throw new InvalidCastException("Not a redis string")
    };

    return new RespInteger(str.Length);
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

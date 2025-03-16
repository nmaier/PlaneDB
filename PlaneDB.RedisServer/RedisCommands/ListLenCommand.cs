using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListLenCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return !client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)
      ? new RespInteger(0)
      : val switch {
        RedisList redisList => new RespInteger(redisList.Count),
        _ => throw new InvalidCastException("Not a redis string")
      };
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

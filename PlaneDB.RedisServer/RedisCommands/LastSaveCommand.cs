using System;

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class LastSaveCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return new RespInteger(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
  }

  public int MaxArgs => 0;
  public int MinArgs => 0;
}

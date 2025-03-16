using System;

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class TimeCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var now = DateTimeOffset.UtcNow;

    return new RespArray(
      new RespInteger(now.ToUnixTimeSeconds()),
      new RespInteger(now.Millisecond));
  }

  public int MaxArgs => 0;
  public int MinArgs => 0;
}

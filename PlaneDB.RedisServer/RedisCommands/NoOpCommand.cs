using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class NoOpCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return RespString.OK;
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 0;
}

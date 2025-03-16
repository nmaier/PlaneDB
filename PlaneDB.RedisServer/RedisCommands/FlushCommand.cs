using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class FlushCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    client.Clear();

    return RespString.OK;
  }

  public int MaxArgs => 0;
  public int MinArgs => 0;
}

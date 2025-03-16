using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class DBSizeCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return new RespInteger(client.Count);
  }

  public int MaxArgs => 0;
  public int MinArgs => 0;
}

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class EchoCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return new RespBulkString(args[0].AsBytes());
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class IncDecrByCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var val = args[1].AsLong();

    return IncDecrCommand.Increment(
      client,
      args[0].AsBytes(),
      cmd == "incrby" ? val : -val);
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

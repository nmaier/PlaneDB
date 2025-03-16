using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class PingCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return args.Length switch {
      1 => new RespString(args[1].AsString()),
      _ => new RespString("PONG")
    };
  }

  public int MaxArgs => 1;
  public int MinArgs => 0;
}

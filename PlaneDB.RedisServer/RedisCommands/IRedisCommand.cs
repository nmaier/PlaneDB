using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal interface IRedisCommand
{
  int MaxArgs { get; }
  int MinArgs { get; }
  RespType Execute(RedisServerClient client, string cmd, RespType[] args);
}

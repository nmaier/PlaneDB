using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class TTLCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return !client.TryGetValue(new RedisKey(args[0].AsBytes()), out var value)
      ? new RespInteger(-2)
      : new RespInteger(
        cmd == "ttl" ? value.ExpiresInSeconds : value.ExpiresInMilliseconds);
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

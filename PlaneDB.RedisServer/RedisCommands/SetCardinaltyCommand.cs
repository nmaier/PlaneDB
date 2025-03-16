using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetCardinaltyCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return !client.TryGetValue(new RedisKey(args[0].AsBytes()), out var item)
      ? new RespInteger(0)
      : new RespInteger(((RedisSet)item).Count);
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

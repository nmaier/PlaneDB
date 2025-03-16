using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class GetCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return !client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)
      ? RespNullString.Value
      : val.StringValue;
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

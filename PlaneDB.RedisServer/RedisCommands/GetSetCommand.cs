using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class GetSetCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    RespType old = RespNullString.Value;
    var val = new RedisString(args[1].AsBytes());
    _ = client.AddOrUpdate(
      new RedisKey(args[0].AsBytes()),
      () => val,
      (in RedisValue value) => {
        old = value.StringValue;

        return val;
      });

    return old;
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

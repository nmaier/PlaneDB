using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class MGetCommand : IRedisCommand
{
  private static RespType GetValue(RedisServerClient client, RespType rawKey)
  {
    return !client.TryGetValue(new RedisKey(rawKey.AsBytes()), out var value)
      ? RespNullString.Value
      : value.StringValue;
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    client.MassInsert(
      () => {
        for (var i = 0; i < args.Length; i++) {
          args[i] = GetValue(client, args[i]);
        }
      });

    return new RespArray(args);
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 1;
}

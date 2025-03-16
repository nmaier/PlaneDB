using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class MSetCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (args.Length % 2 != 0) {
      throw RespResponseException.WrongNumberOfArguments;
    }

    client.MassInsert(
      () => {
        for (var i = 0; i < args.Length; i += 2) {
          var key = args[i].AsBytes();
          var val = args[i + 1].AsBytes();
          client.SetValue(new RedisKey(key), new RedisString(val));
        }
      });

    return RespString.OK;
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

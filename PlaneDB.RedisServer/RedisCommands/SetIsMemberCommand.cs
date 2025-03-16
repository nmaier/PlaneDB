using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetIsMemberCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    if (cmd == "sismember" && args.Length > 2) {
      throw RespResponseException.WrongNumberOfArguments;
    }

    if (!client.TryGetValue(new RedisKey(key), out var item)) {
      return new RespArray();
    }

    var set = (RedisSet)item;
    RespType one = new RespInteger(1);
    RespType zero = new RespInteger(0);
    var rv = new RespArray(
      args.Skip(1).Select(a => set.Contains(client, key, a.AsBytes()) ? one : zero));

    return cmd switch {
      "sismember" => new RespInteger(rv[0].AsLong()),
      _ => rv
    };
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

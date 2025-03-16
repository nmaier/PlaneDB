using System.Diagnostics.CodeAnalysis;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListIndexCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var index = args[1].AsLong();
    RespType rv = RespNullString.Value;
    var key = args[0].AsBytes();
    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey _,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue value) => {
        var list = (RedisList)existing;
        if (list.TryGetValue(client, key, index, out var val)) {
          rv = new RespBulkString(val.Value);
        }

        value = null!;

        return false;
      });

    return rv;
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

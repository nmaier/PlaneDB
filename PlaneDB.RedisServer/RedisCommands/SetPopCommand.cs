using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetPopCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var pop = cmd == "spop";
    var count = args.Length switch {
      1 => 1,
      _ => args[1].AsLong()
    };

    var removed = Array.Empty<RespType>();
    var exists = false;
    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey redisKey,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue value) => {
        var set = (RedisSet)existing;
        exists = true;
        removed =
          (pop ? set.PopRandom(client, key, count) : set.PeekRandom(client, key, count))
          .Select(RespType (s) => new RespBulkString(s.Value))
          .ToArray();
        if (set.Count == 0) {
          _ = client.TryRemove(redisKey, out _);
          value = null!;

          return false;
        }

        value = set;

        return true;
      });

    return args.Length switch {
      1 when removed.Length > 0 => removed[0],
      1 => RespNullString.Value,
      _ when exists => new RespArray(removed),
      _ => RespNullArray.Value
    };
  }

  public int MaxArgs => 2;
  public int MinArgs => 1;
}

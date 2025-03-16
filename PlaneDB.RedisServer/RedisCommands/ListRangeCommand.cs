using System.Diagnostics.CodeAnalysis;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListRangeCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var start = args[1].AsLong();
    var end = args[2].AsLong();
    var rv = new RespArray();
    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey redisKey,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue newValue) => {
        newValue = null!;
        var list = (RedisList)existing;
        if (list.Count == 0) {
          _ = client.TryRemove(redisKey, out _);

          return false;
        }

        if (start < 0) {
          start = list.Count + start;
        }

        if (start < 0) {
          start = 0;
        }

        if (start >= list.Count) {
          start = list.Count - 1;
        }

        if (end < 0) {
          end = list.Count + end;
        }

        if (end < 0) {
          end = 0;
        }

        if (end >= list.Count) {
          end = list.Count;
        }

        if (end == start) {
          return false;
        }

        if (start == 0 && end == list.Count - 1) {
          rv = new RespArray(
            list.EnumerateForward(client, key, list.Count)
              .Select(n => new RespBulkString(n.Value)));

          return false;
        }

        rv = new RespArray(
          list.EnumerateForward(client, key, end + 1)
            .Skip((int)start)
            .Select(n => new RespBulkString(n.Value)));

        return false;
      });

    return rv;
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListPopCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var count = 1L;
    var asArray = false;
    if (args.Length > 1) {
      count = args[1].AsLong();
      switch (count) {
        case < 0:
          throw new RespResponseException("Invalid COUNT");
        case 0:
          return new RespArray();
        default:
          asArray = true;

          break;
      }
    }

    var rv = Array.Empty<RespType>();

    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey rkey,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue value) => {
        var list = (RedisList)existing;
        if (list.Count == 0) {
          _ = client.TryRemove(rkey, out _);
          value = null!;

          return false;
        }

        rv = (cmd switch {
            "rpop" => list.PopBack(client, key, count),
            _ => list.PopFront(client, key, count)
          }).Select(RespType (e) => new RespBulkString(e.Value))
          .ToArray();
        if (list.Count == 0) {
          _ = client.TryRemove(rkey, out _);
          value = null!;

          return false;
        }

        Debug.WriteLine($"List Count: {list}");
        value = list;

        return true;
      });

    return !asArray ? rv.Length > 0 ? rv[0] : RespNullString.Value :
      rv.Length > 0 ? new RespArray(rv) : RespNullArray.Value;
  }

  public int MaxArgs => 2;
  public int MinArgs => 1;
}

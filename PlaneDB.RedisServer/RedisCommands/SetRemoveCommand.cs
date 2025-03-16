using System.Diagnostics.CodeAnalysis;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetRemoveCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var removed = 0L;
    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey redisKey,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue value) => {
        var set = (RedisSet)existing;
        removed = args.Skip(1).LongCount(v => set.Remove(client, key, v.AsBytes()));
        if (set.Count == 0) {
          _ = client.TryRemove(redisKey, out _);
          value = null!;

          return false;
        }

        value = set;

        return true;
      });

    return new RespInteger(removed);
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

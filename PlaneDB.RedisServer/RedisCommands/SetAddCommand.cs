using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetAddCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var added = 0L;
    _ = client.AddOrUpdate(
      new RedisKey(key),
      () => {
        var set = new RedisSet(client);
        added = args.Skip(1).LongCount(v => set.Add(client, key, v.AsBytes()));

        return set;
      },
      (in RedisValue existing) => {
        var set = (RedisSet)existing;
        added = args.Skip(1).LongCount(v => set.Add(client, key, v.AsBytes()));

        return set;
      });

    return new RespInteger(added);
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

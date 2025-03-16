using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class DelCommand : IRedisCommand
{
  private static bool TryDeleteKey(RedisServerClient client, byte[] key)
  {
    return client.TryRemove(new RedisKey(key), out _);
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var deleted = args.LongCount(key => TryDeleteKey(client, key.AsBytes()));

    return new RespInteger(deleted);
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 1;
}

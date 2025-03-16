using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ExistsCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var count = args.LongCount(a => client.TryGetValue(new RedisKey(a.AsBytes()), out _));

    return new RespInteger(count);
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 1;
}

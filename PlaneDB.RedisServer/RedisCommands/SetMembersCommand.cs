using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetMembersCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();

    return client.MassInsert(
      () => !client.TryGetValue(new RedisKey(key), out var item) ||
            item is not RedisSet set
        ? new RespArray()
        : new RespArray(
          set.Enumerate(client, key).Select(s => new RespBulkString(s.Value))));
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

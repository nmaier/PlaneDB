using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListSetCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = new RedisKey(args[0].AsBytes());
    var offset = args[1].AsLong();
    var value = args[2].AsBytes();

    _ = client.AddOrUpdate(
      key,
      () => throw new RespResponseException("List does not exist"),
      Updater);

    return RespString.OK;

    RedisValue Updater(in RedisValue existing)
    {
      var list = (RedisList)existing;
      if (list.Count == 0) {
        _ = client.TryRemove(key, out _);

        throw new RespResponseException("List does not exist");
      }

      list.Replace(client, key.KeyBytes, offset, value);

      return list;
    }
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

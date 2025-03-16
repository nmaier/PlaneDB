using System.Collections.Generic;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class MSetNXCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (args.Length % 2 != 0) {
      throw RespResponseException.WrongNumberOfArguments;
    }

    var pending = new List<KeyValuePair<byte[], byte[]>>();

    return client.MassInsert(
      () => {
        for (var i = 0; i < args.Length; i += 2) {
          var key = args[i].AsBytes();
          var val = args[i + 1].AsBytes();
          if (client.TryGetValue(new RedisKey(key), out _)) {
            return new RespInteger(0);
          }

          pending.Add(new KeyValuePair<byte[], byte[]>(key, val));
        }

        foreach (var (key, value) in pending) {
          client.SetValue(new RedisKey(key), new RedisString(value));
        }

        return new RespInteger(1);
      });
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

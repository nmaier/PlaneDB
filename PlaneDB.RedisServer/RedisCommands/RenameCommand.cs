using System.Collections.Generic;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class RenameCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var from = new RedisKey(args[0].AsBytes());
    var to = new RedisKey(args[1].AsBytes());
    switch (cmd) {
      case "rename":
        client.MassInsert(
          () => {
            if (!client.TryRemove(from, out var val)) {
              _ = client.TryRemove(to, out _);
            }
            else {
              client.SetValue(to, val);
            }
          });

        return RespString.OK;

      case "renamenx":
        try {
          _ = client.AddOrUpdate(
            to,
            () => !client.TryRemove(from, out var existing)
              ? throw new KeyNotFoundException()
              : existing,
            (in RedisValue _) => throw new KeyNotFoundException());

          return new RespInteger(1);
        }
        catch (KeyNotFoundException) {
          return new RespInteger(0);
        }

      default: throw new RespResponseException("Invalid rename variant");
    }
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

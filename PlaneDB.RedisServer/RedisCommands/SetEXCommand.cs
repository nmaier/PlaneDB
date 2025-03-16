using System;

using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetEXCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var seconds = Math.Max(-1, args[1].AsLong());
    var milliseconds = cmd switch {
      "psetex" => seconds,
      _ => (long)TimeSpan.FromSeconds(seconds).TotalMilliseconds
    };

    return SetCommand.ApplyNormal(
      client,
      args[0].AsBytes(),
      args[2].AsBytes(),
      new SetCommand.Options {
        Expires = milliseconds,
        ExpiresMode = SetCommand.ExpiresMode.Explicit
      });
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

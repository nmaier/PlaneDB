using System;
using System.Diagnostics.CodeAnalysis;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ExpireCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var seconds = cmd switch {
      "persist" when args.Length is 1 => 0,
      _ when args.Length is 2 => Math.Max(-1, args[1].AsLong()),
      _ => throw RespResponseException.WrongNumberOfArguments
    };
    var key = args[0].AsBytes();
    var now = DateTimeOffset.UtcNow;

    var ts = cmd switch {
      "persist" => -1L,
      "expire" => (now + TimeSpan.FromSeconds(seconds)).Ticks,
      "pexpire" => (now + TimeSpan.FromMilliseconds(seconds)).Ticks,
      "expireat" => DateTimeOffset.FromUnixTimeSeconds(seconds).Ticks,
      "pexpireat" => DateTimeOffset.FromUnixTimeMilliseconds(seconds).Ticks,
      _ => throw new RespResponseException("Unhandled expire variant")
    };

    switch (ts) {
      case >= 0 when ts < now.Ticks:
        _ = client.TryRemove(new RedisKey(key), out _);

        return new RespInteger(0);
      default:
        return client.TryUpdate(
          new RedisKey(key),
          (
            in RedisKey _,
            in RedisValue existing,
            [MaybeNullWhen(false)] out RedisValue value) => {
            existing.Expires = ts;
            value = existing;

            return true;
          })
          ? new RespInteger(1)
          : new RespInteger(0);
    }
  }

  public int MaxArgs => 2;
  public int MinArgs => 1;
}

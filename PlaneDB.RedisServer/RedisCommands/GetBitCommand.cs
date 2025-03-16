using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class GetBitCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var pos = args[1].AsLong();
    if (pos is < 0 or >= 536870912) {
      throw new RespResponseException("Invalid position");
    }

    if (!client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)) {
      return new RespInteger(0);
    }

    var str = val switch {
      RedisInteger redisInteger => redisInteger.StringValue.AsBytes(),
      RedisString redisString => redisString.Value,
      _ => throw new InvalidCastException("Not a redis string")
    };

    var bpos = pos / 8;
    pos %= 8;

    return bpos >= str.Length
      ? new RespInteger(0)
      : new RespInteger((str[bpos] >> (7 - (byte)pos)) & 1);
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

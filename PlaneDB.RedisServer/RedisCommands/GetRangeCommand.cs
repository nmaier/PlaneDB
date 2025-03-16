using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class GetRangeCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (!client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)) {
      return new RespBulkString("");
    }

    var str = val switch {
      RedisInteger redisInteger => redisInteger.StringValue.AsBytes(),
      RedisString redisString => redisString.Value,
      _ => throw new InvalidCastException("Not a redis string")
    };
    var sp = str.AsSpan();

    var start = args[1].AsLong();
    var end = args[2].AsLong();
    switch (start) {
      case < 0 when end < 0 && start > end:
        return new RespBulkString("");
      case 0 when end is -1:
        return sp.Length <= 0 ? new RespBulkString("") : new RespBulkString(sp.ToArray());
      case < 0:
        start = str.Length + start;

        break;
    }

    if (start < 0) {
      start = 0;
    }

    if (end < 0) {
      end = str.Length + end;
    }

    if (end < 0) {
      end = 0;
    }

    if (end >= str.Length) {
      end = str.Length - 1;
    }

    if (start >= 4294967296L) {
      throw new RespResponseException("Invalid range");
    }

    if (start > end) {
      return new RespBulkString("");
    }

    sp = sp.Slice((int)start, (int)(end - start + 1));

    return sp.Length <= 0 ? new RespBulkString("") : new RespBulkString(sp.ToArray());
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

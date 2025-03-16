using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetRangeCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var start = args[1].AsLong();
    var bytes = args[2].AsBytes();

    var rv = client.AddOrUpdate(new RedisKey(args[0].AsBytes()), Adder, Updater);

    return new RespInteger(((RedisString)rv).Value.Length);

    static RedisString MakeString(byte[] existing, byte[] value, long offset)
    {
      if (offset < 0) {
        offset = existing.Length + offset;
      }

      if (offset < 0) {
        offset = 0;
      }

      if (offset + value.Length >= 4294967296L) {
        throw new RespResponseException("Invalid range");
      }

      if (existing.Length < offset + value.Length) {
        var ns = new byte[offset + value.Length];
        existing.AsSpan().CopyTo(ns);
        existing = ns;
      }

      var sp = existing.AsSpan((int)offset);
      value.AsSpan().CopyTo(sp);

      return new RedisString(existing);
    }

    RedisValue Adder()
    {
      return MakeString([], bytes, start);
    }

    RedisValue Updater(in RedisValue existing)
    {
      var str = existing switch {
        RedisInteger redisInteger => redisInteger.StringValue.AsBytes(),
        RedisString redisString => redisString.Value,
        _ => throw new InvalidCastException("Not a redis string")
      };

      return MakeString(str, bytes, start);
    }
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

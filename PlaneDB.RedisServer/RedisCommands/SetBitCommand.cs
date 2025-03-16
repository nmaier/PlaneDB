using System;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetBitCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var pos = args[1].AsLong();
    if (pos is < 0 or >= 536870912) {
      throw new RespResponseException("Invalid position");
    }

    var bit = args[2].AsLong();
    if (bit is not 1 and not 0) {
      throw new RespResponseException("Invalid bit");
    }

    var bpos = pos / 8;
    pos %= 8;

    _ = client.AddOrUpdate(
      new RedisKey(args[0].AsBytes()),
      InitBuffer,
      (in RedisValue value) => {
        var str = value switch {
          RedisInteger redisInteger => new RedisString(
            redisInteger.StringValue.AsBytes(),
            value.Expires),
          RedisString redisString => redisString,
          _ => throw new InvalidCastException("Not a redis string")
        };

        var buf = (str.Value.Length > bpos) switch {
          true => str.Value,
          _ => ResizeBuffer(str.Value)
        };

        if (bit == 0) {
          bit = (buf[bpos] >> (7 - (byte)pos)) & 1;
          buf[bpos] &= (byte)~(1 << (7 - (byte)pos));
        }
        else {
          bit = (buf[bpos] >> (7 - (byte)pos)) & 1;
          buf[bpos] |= (byte)(1 << (7 - (byte)pos));
        }

        return new RedisString(buf, value.Expires);
      });

    return new RespInteger(bit);

    RedisValue InitBuffer()
    {
      var buf = new byte[bpos + 1];
      if (bit != 0) {
        buf[bpos] |= (byte)(1 << (7 - (byte)pos));
      }

      bit = 0;

      return new RedisString(buf);
    }

    byte[] ResizeBuffer(byte[] src)
    {
      var rv = new byte[bpos + 1];
      src.AsSpan().CopyTo(rv);

      return rv;
    }
  }

  public int MaxArgs => 3;
  public int MinArgs => 3;
}

using System;
using System.Globalization;
using System.Linq;
using System.Text;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class AppendCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = args[0].AsBytes();
    var val = args[1].AsBytes();
    var rv = client.AddOrUpdate(
      new RedisKey(key),
      () => new RedisString(val),
      (in RedisValue value) => {
        var newValue = value switch {
          RedisInteger ri => new RedisString(
            Encoding.UTF8.GetBytes(ri.Value.ToString(CultureInfo.InvariantCulture))
              .Concat(val)
              .ToArray(),
            value.Expires),
          RedisString rs => new RedisString(
            rs.Value.Concat(val).ToArray(),
            value.Expires),
          RedisNull => new RedisString(val, value.Expires),
          _ => throw new InvalidCastException()
        };

        return newValue.Value.Length <= 536870912
          ? newValue
          : throw new RespResponseException("string size overflow");
      });

    return new RespInteger(((RedisString)rv).Value.Length);
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

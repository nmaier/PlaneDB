using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListPushCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var key = new RedisKey(args[0].AsBytes());
    var front = cmd switch {
      "lpush" => true,
      "lpushx" => true,
      "rpush" => false,
      "rpushx" => false,
      _ => throw RespResponseException.WrongNumberOfArguments
    };

    var onlyExisting = cmd is "lpushx" or "rpushx";

    try {
      var rv = (RedisList)client.AddOrUpdate(
        key,
        () => {
          if (onlyExisting) {
            throw new KeyNotFoundException();
          }

          var list = new RedisList();
          Debug.WriteLine($"List create {list}");
          if (front) {
            list.PushFront(
              client,
              key.KeyBytes,
              args.Skip(1).Select(a => a.AsBytes()).ToArray());
          }
          else {
            list.PushBack(
              client,
              key.KeyBytes,
              args.Skip(1).Select(a => a.AsBytes()).ToArray());
          }

          return list;
        },
        (in RedisValue existing) => {
          var list = (RedisList)existing;
          Debug.WriteLine($"List update {existing.Expired} -> {list}");
          if (front) {
            list.PushFront(
              client,
              key.KeyBytes,
              args.Skip(1).Select(a => a.AsBytes()).ToArray());
          }
          else {
            list.PushBack(
              client,
              key.KeyBytes,
              args.Skip(1).Select(a => a.AsBytes()).ToArray());
          }

          return list;
        });

      return new RespInteger(rv.Count);
    }
    catch (KeyNotFoundException) {
      return new RespInteger(0);
    }
  }

  public int MaxArgs => int.MaxValue;
  public int MinArgs => 2;
}

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class ListPosCommand : IRedisCommand
{
  private static readonly IPlaneByteArrayComparer cmp = PlaneByteArrayComparer.Default;

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (args.Length % 2 != 0) {
      throw RespResponseException.WrongNumberOfArguments;
    }

    var key = args[0].AsBytes();
    var needle = args[1].AsBytes();

    var rank = 1L;
    var count = -1L;
    var maxLen = 0L;
    var matches = new List<long>();
    var remainderArgs = args.AsSpan(2);
    while (remainderArgs.Length > 0) {
      switch (remainderArgs[0].AsString().ToLowerInvariant()) {
        case "count":
          count = remainderArgs[1].AsLong();
          if (count < 0) {
            throw new RespResponseException("Invalid COUNT");
          }

          break;
        case "rank":
          rank = remainderArgs[1].AsLong();
          if (rank == 0) {
            throw new RespResponseException("Invalid RANK");
          }

          break;
        case "maxlen":
          maxLen = remainderArgs[1].AsLong();
          if (maxLen < 0) {
            throw new RespResponseException("Invalid MAXLEN");
          }

          break;
        default:
          throw new RespResponseException("Invalid argument");
      }

      remainderArgs = remainderArgs[2..];
    }

    _ = client.TryUpdate(
      new RedisKey(key),
      (
        in RedisKey redisKey,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue newValue) => {
        var list = (RedisList)existing;

        if (list.Count == 0) {
          _ = client.TryRemove(redisKey, out _);
          newValue = null!;

          return false;
        }

        if (maxLen <= 0) {
          maxLen = list.Count;
        }

        var enumerable = rank switch {
          > 0 => list.EnumerateForward(client, key, maxLen)
            .Select((e, i) => new KeyValuePair<int, RedisListNode>(i, e))
            .ToArray(),
          _ => list.EnumerateReverse(client, key, maxLen)
            .Select(
              (e, i) =>
                new KeyValuePair<int, RedisListNode>((int)(list.Count - i - 1), e))
            .ToArray()
        };
        rank = Math.Abs(rank);

        foreach (var (idx, value) in enumerable) {
          if (!cmp.Equals(needle, value.Value)) {
            continue;
          }

          if (rank > 1) {
            --rank;

            continue;
          }

          matches.Add(idx);
          if (count > 0 && matches.Count >= count) {
            break;
          }
        }

        newValue = null!;

        return false;
      });

    return count == -1
      ? matches.Count > 0 ? new RespInteger(matches[0]) : RespNullString.Value
      : new RespArray(matches.Select(i => new RespInteger(i)));
  }

  public int MaxArgs => 8;
  public int MinArgs => 2;
}

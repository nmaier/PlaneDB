using System;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class BitCountCommand : IRedisCommand
{
  private static long PopCount(in ReadOnlySpan<ulong> full)
  {
    if (full.Length < 0) {
      return 0;
    }

    ulong rv = 0;
    if (Popcnt.X64.IsSupported) {
      for (int index = 0, end = full.Length; index < end; index++) {
        var l = full[index];
        rv += Popcnt.X64.PopCount(l);
      }

      return (long)rv;
    }

    for (int index = 0, end = full.Length; index < end; index++) {
      var l = full[index];
      for (; l != 0; rv++) {
        l &= l - 1;
      }
    }

    return (long)rv;
  }

  private static int PopCount(in ReadOnlySpan<uint> half)
  {
    if (half.Length < 0) {
      return 0;
    }

    uint rv = 0;
    if (Popcnt.IsSupported) {
      for (int index = 0, end = half.Length; index < end; index++) {
        var l = half[index];
        rv += Popcnt.PopCount(l);
      }

      return (int)rv;
    }

    for (int index = 0, end = half.Length; index < end; index++) {
      var l = half[index];
      for (; l != 0; rv++) {
        l &= l - 1;
      }
    }

    return (int)rv;
  }

  private static int PopCount(in ReadOnlySpan<byte> bytes)
  {
    if (bytes.Length < 0) {
      return 0;
    }

    uint rv = 0;
    if (Popcnt.IsSupported) {
      for (int index = 0, end = bytes.Length; index < end; index++) {
        uint l = bytes[index];
        rv += Popcnt.PopCount(l);
      }

      return (int)rv;
    }

    for (int index = 0, end = bytes.Length; index < end; index++) {
      int l = bytes[index];
      for (; l != 0; rv++) {
        l &= l - 1;
      }
    }

    return (int)rv;
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    if (args.Length is not 1 and not 3) {
      throw RespResponseException.WrongNumberOfArguments;
    }

    if (!client.TryGetValue(new RedisKey(args[0].AsBytes()), out var val)) {
      return new RespInteger(0);
    }

    var str = val switch {
      RedisInteger redisInteger => redisInteger.StringValue.AsBytes(),
      RedisString redisString => redisString.Value,
      _ => throw new InvalidCastException("Not a redis string")
    };
    var sp = str.AsSpan();

    if (args.Length == 3) {
      var start = args[1].AsLong();
      var end = args[2].AsLong();
      if (start < 0 && end < 0 && start > end) {
        return new RespInteger(0);
      }

      if (start is not 0 || end is not -1) {
        if (start < 0) {
          start = str.Length + start;
        }

        if (end < 0) {
          end = str.Length + end;
        }

        if (start < 0) {
          start = 0;
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
          return new RespInteger(0);
        }

        sp = sp.Slice((int)start, (int)(end - start + 1));
      }
    }

    if (sp.Length <= 0) {
      return new RespInteger(0);
    }

    var rv = 0L;
    var full = MemoryMarshal.Cast<byte, ulong>(sp[..^(sp.Length % sizeof(ulong))]);
    sp = sp[^(sp.Length % sizeof(ulong))..];
    if (full.Length > 0) {
      rv += PopCount(full);
    }

    var half = MemoryMarshal.Cast<byte, uint>(sp[..^(sp.Length % sizeof(uint))]);
    sp = sp[^(sp.Length % sizeof(uint))..];
    if (half.Length > 0) {
      rv += PopCount(half);
    }

    if (sp.Length > 0) {
      rv += PopCount(sp);
    }

    return new RespInteger(rv);
  }

  public int MaxArgs => 3;
  public int MinArgs => 1;
}

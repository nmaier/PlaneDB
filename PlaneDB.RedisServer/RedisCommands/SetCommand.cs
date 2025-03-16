using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

[PublicAPI]
internal sealed class SetCommand : IRedisCommand
{
  internal static RespType Apply(
    RedisServerClient client,
    byte[] key,
    byte[] val,
    in Options options)
  {
    return options.Mode switch {
      Mode.Normal => ApplyNormal(client, key, val, options),
      Mode.NX => ApplyNX(client, key, val, options),
      Mode.XX => ApplyXX(client, key, val, options),
      _ => throw new InvalidCastException()
    };
  }

  internal static RespType ApplyNormal(
    RedisServerClient client,
    byte[] key,
    byte[] val,
    Options options)
  {
    if (val.Length > 536870912) {
      throw new RespResponseException("Invalid length");
    }

    switch (options.ExpiresMode) {
      case ExpiresMode.Normal: {
        if (options.ReturnValue) {
          RespType existing = RespNullString.Value;
          _ = client.AddOrUpdate(
            new RedisKey(key),
            () => new RedisString(val),
            (in RedisValue current) => {
              existing = current.StringValue;

              return new RedisString(val);
            });

          return existing;
        }

        client.SetValue(new RedisKey(key), new RedisString(val));

        return RespString.OK;
      }

      case ExpiresMode.KeepTTL: {
        RespType existing = RespNullString.Value;
        _ = client.AddOrUpdate(
          new RedisKey(key),
          () => new RedisString(val),
          (in RedisValue value) => {
            if (options.ReturnValue) {
              existing = value.StringValue;
            }

            return new RedisString(val, value.Expires);
          });

        return options.ReturnValue ? existing : RespString.OK;
      }

      case ExpiresMode.Explicit: {
        if (options.Expires <= 0) {
          return !client.TryRemove(new RedisKey(key), out var existingExpires)
            ?
            RespNullString.Value
            : options.ReturnValue
              ? existingExpires.StringValue
              : RespString.OK;
        }

        var sval = new RedisString(
          val,
          (DateTime.UtcNow + TimeSpan.FromMilliseconds(options.Expires)).Ticks);
        if (options.ReturnValue) {
          RespType existing = RespNullString.Value;
          _ = client.AddOrUpdate(
            new RedisKey(key),
            () => sval,
            (in RedisValue value) => {
              existing = value.StringValue;

              return sval;
            });

          return existing;
        }

        client.SetValue(new RedisKey(key), sval);

        return RespString.OK;
      }

      default:
        throw new InvalidCastException();
    }
  }

  internal static RespType ApplyNX(
    RedisServerClient client,
    byte[] key,
    byte[] val,
    in Options options)
  {
    try {
      _ = client.AddOrUpdate(
        new RedisKey(key),
        () => new RedisString(val),
        (in RedisValue _) => throw new KeyNotFoundException());

      return options.ReturnValue ? RespNullString.Value : RespString.OK;
    }
    catch (KeyNotFoundException) {
      return RespNullString.Value;
    }
  }

  internal static RespType ApplyXX(
    RedisServerClient client,
    byte[] key,
    byte[] val,
    Options options)
  {
    if (options is { ExpiresMode: ExpiresMode.Explicit, Expires: <= 0 }) {
      return client.TryRemove(new RedisKey(key), out var existingValue)
        ? options.ReturnValue ? existingValue.StringValue : RespString.OK
        : RespNullString.Value;
    }

    RespType existing = RespNullString.Value;

    return !client.TryUpdate(
        new RedisKey(key),
        (
          in RedisKey _,
          in RedisValue value,
          [MaybeNullWhen(false)] out RedisValue newValue) => {
          if (options.ReturnValue) {
            existing = value.StringValue;
          }

          var seconds = options.ExpiresMode switch {
            ExpiresMode.Normal => -1,
            ExpiresMode.KeepTTL => value.Expires,
            ExpiresMode.Explicit => (DateTime.UtcNow +
                                     TimeSpan.FromSeconds(options.Expires)).Ticks,
            _ => throw new InvalidCastException()
          };
          newValue = new RedisString(val, seconds);

          return true;
        }) ? RespNullString.Value :
      options.ReturnValue ? existing : RespString.OK;
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var options = new Options();
    var key = args[0].AsBytes();
    var val = args[1].AsBytes();
    for (var i = 2; i < args.Length; i++) {
      var v = args[i].AsString().ToLowerInvariant();
      switch (v) {
        case "nx":
        case "xx":
          if (options.Mode != Mode.Normal) {
            throw new RespResponseException("Cannot set NX and XX at the same time");
          }

          options.Mode = v == "nx" ? Mode.NX : Mode.XX;

          break;

        case "keepttl":
          if (options.ExpiresMode != ExpiresMode.Normal) {
            throw new RespResponseException(
              "Cannot set KEEPTTL, EX, PX at the same time");
          }

          options.ExpiresMode = ExpiresMode.KeepTTL;

          break;

        case "px":
        case "ex":
          if (options.ExpiresMode != ExpiresMode.Normal) {
            throw new RespResponseException(
              "Cannot set KEEPTTL, EX, PX at the same time");
          }

          if (i == args.Length - 1) {
            throw new RespResponseException("No duration provided");
          }

          options.Expires = v == "px"
            ? args[i + 1].AsLong()
            : (long)TimeSpan.FromSeconds(args[i + 1].AsLong()).TotalMilliseconds;
          options.ExpiresMode = ExpiresMode.Explicit;
          ++i;

          break;

        case "get":
          options.ReturnValue = true;

          break;

        default:
          throw new RespResponseException("Invalid Set options");
      }
    }

    return Apply(client, key, val, options);
  }

  public int MaxArgs => 6;
  public int MinArgs => 2;

  internal enum ExpiresMode
  {
    Normal,
    KeepTTL,
    Explicit
  }

  internal enum Mode
  {
    Normal,
    NX,
    XX
  }

  internal struct Options
  {
    internal bool ReturnValue;
    internal Mode Mode;
    internal ExpiresMode ExpiresMode;
    internal long Expires;
  }
}

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB;

internal sealed class RedisServerClient
{
  // ReSharper disable once CollectionNeverUpdated.Local
  private static readonly RedisCommandRegistry commands = [];
  private readonly byte[] authToken;
  private readonly IPlaneDB<RedisKey, RedisValue> db;
  private readonly RespParser input;
  private readonly RespParser output;
  private readonly CancellationToken token;
  private bool authenticated;

  internal RedisServerClient(
    IPlaneDB<RedisKey, RedisValue> db,
    Stream inStream,
    Stream outStream,
    string authToken,
    CancellationToken token)
  {
    this.db = db;
    this.authToken = string.IsNullOrEmpty(authToken) ? [] : authToken.AuthHash();
    input = new RespParser(inStream);
    output = new RespParser(outStream);
    this.token = token;
  }

  public int Count => db.Count;
  public IEnumerable<RedisKey> KeysIterator => db.KeysIterator;

  internal RedisValue AddOrUpdate(
    RedisKey key,
    [InstantHandle] IPlaneDictionary<RedisKey, RedisValue>.ValueFactory addFactory,
    [InstantHandle]
    IPlaneDictionary<RedisKey, RedisValue>.UpdateValueFactory updateFactory)
  {
    return db.AddOrUpdate(
      key,
      addFactory,
      (in RedisValue existing) => {
        if (!existing.Expired) {
          return updateFactory(existing);
        }

        try {
          return addFactory();
        }
        catch {
          _ = TryRemove(key, out _);

          throw;
        }
      });
  }

  public void Clear()
  {
    db.Clear();
  }

  public void MassInsert([InstantHandle] Action action)
  {
    db.MassInsert(action);
  }

  public TResult MassInsert<TResult>([InstantHandle] Func<TResult> action)
  {
    return db.MassInsert(action);
  }

  internal async Task Serve()
  {
    while (!token.IsCancellationRequested) {
      try {
        var cmd = await input.ReadNext(token);
        if (cmd is not RespArray cmdArray || cmdArray.Length < 1) {
          await output.Write(new RespErrorString("ERR Invalid command format"), token);

          continue;
        }

        Debug.WriteLine($"Req: [{cmdArray}]");

        var cmdId = cmdArray.Elements[0].AsString().ToLowerInvariant();

        if (StringComparer.OrdinalIgnoreCase.Equals("quit", cmdId)) {
          await output.Write(RespString.OK, token);

          return;
        }

        if (StringComparer.OrdinalIgnoreCase.Equals("auth", cmdId)) {
          var pw = cmdArray.Elements[1].AsBytes().AuthHash();
          authenticated = authToken.ConstantTimeEquals(pw);
          await output.Write(
            authenticated
              ? RespString.OK
              : new RespErrorString("ERR authentication failure"),
            token);
          Debug.WriteLine("Res: <authenticated>");

          continue;
        }

        if (!authenticated && authToken.Length > 0) {
          await output.Write(new RespErrorString("ERR Not authenticated"), token);

          continue;
        }

        if (!commands.TryGetValue(cmdId, out var command)) {
          Debug.WriteLine($"Res: {cmdId} -> <unhandled>");
          await output.Write(
            new RespErrorString($"ERR Unsupported command {cmdId}"),
            token);

          continue;
        }

        var args = cmdArray.Elements.AsSpan(1).ToArray();
        if (args.Length < command.MinArgs || args.Length > command.MaxArgs) {
          throw RespResponseException.WrongNumberOfArguments;
        }

        var resp = command.Execute(this, cmdId, args);
        Debug.WriteLine($"Res: {cmdId} [{cmdArray}] -> {resp}");
        await output.Write(resp, token);
      }
      catch (InvalidCastException) {
        await output.Write(
          new RespErrorString("ERR Malformed command invocation"),
          token);
      }
      catch (FormatException ex) {
        await output.Write(new RespErrorString($"ERR {ex.Message}"), token);
      }
      catch (RespResponseException ex) {
        await output.Write(new RespErrorString($"ERR {ex.Message}"), token);
      }
      catch (Exception ex) {
        try {
          await output.Write(new RespErrorString($"ERR {ex.Message}"), token);
        }
        catch {
          // ignored
        }

        return;
      }
    }
  }

  internal void SetValue(in RedisKey key, RedisValue value)
  {
    db.SetValue(key, value);
  }

  public bool TryAdd(
    RedisKey key,
    [InstantHandle] IPlaneDictionary<RedisKey, RedisValue>.ValueFactory valueFactory)
  {
    var added = false;
    _ = AddOrUpdate(
      key,
      () => {
        added = true;

        return valueFactory();
      },
      (in RedisValue v) => v);

    return added;
  }

  internal bool TryGetValue(RedisKey key, [MaybeNullWhen(false)] out RedisValue value)
  {
    var res = false;
    RedisValue rv = null!;
    _ = TryUpdate(
      key,
      (
        in RedisKey _,
        in RedisValue existing,
        [MaybeNullWhen(false)] out RedisValue newValue) => {
        res = true;
        rv = existing;
        newValue = null!;

        return false;
      });
    value = rv;

    return res;
  }

  internal bool TryRemove(RedisKey key, [MaybeNullWhen(false)] out RedisValue value)
  {
    if (!db.TryRemove(key, out value)) {
      return false;
    }

    switch (value) {
      case RedisInteger:
      case RedisNull:
      case RedisString:
      case RedisListNode:
      case RedisSetNode:
        break;
      case RedisSet set:
        foreach (var redisString in set.Enumerate(this, key.KeyBytes).ToArray()) {
          _ = set.Remove(this, key.KeyBytes, redisString.Value);
        }

        break;
      case RedisList list:
        list.Clear(this, key);

        break;
      default:
        throw new InvalidOperationException();
    }

    return !value.Expired;
  }

  internal bool TryUpdate(
    RedisKey key,
    [InstantHandle] IPlaneDictionary<RedisKey, RedisValue>.TryUpdateFactory factory)
  {
    return db.TryUpdate(key, UpdateFactory);

    bool UpdateFactory(
      in RedisKey __,
      in RedisValue existing,
      [MaybeNullWhen(false)] out RedisValue newValue)
    {
      if (!existing.Expired) {
        return factory(key, existing, out newValue);
      }

      _ = TryRemove(key, out _);
      newValue = null!;

      return false;
    }
  }
}

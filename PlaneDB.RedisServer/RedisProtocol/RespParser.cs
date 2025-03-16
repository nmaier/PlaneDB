using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NMaier.PlaneDB.RedisProtocol;

internal sealed class RespParser
{
  private static readonly byte[] trailer = "\r\n"u8.ToArray();
  private readonly byte[] buf = new byte[1];
  private readonly Stream stream;
  private int nesting;
  private int reading;
  private int writing;

  internal RespParser(Stream stream)
  {
    this.stream = stream;
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task<byte> ReadByteAsync(CancellationToken token)
  {
    return await stream.ReadAsync(buf.AsMemory(), token).ConfigureAwait(false) != 1
      ? throw new RespProtocolException("Truncated RESP message")
      : buf[0];
  }

  private async ValueTask<int> ReadInteger(CancellationToken token)
  {
    var neg = false;
    var v = 0;
    var first = true;
    while (true) {
      var c = await ReadByteAsync(token).ConfigureAwait(false);
      switch (c) {
        case (byte)'-' when first:
          neg = true;

          break;
        case (byte)'+' when first:
          break;
        case >= (byte)'0' and <= (byte)'9':
          try {
            checked {
              v = (v * 10) + c - (byte)'0';
            }
          }
          catch (OverflowException) {
            throw new RespProtocolException("Integer overflow");
          }

          break;
        case (byte)'\r':
          if (await ReadByteAsync(token).ConfigureAwait(false) != '\n') {
            throw new RespProtocolException("Invalid RESP integer");
          }

          return neg ? -v : v;

        default:
          throw new RespProtocolException("Invalid RESP integer");
      }

      first = false;
    }
  }

  private async Task<RespType> ReadNewArray(CancellationToken token)
  {
    if (nesting >= 32) {
      throw new RespProtocolException("RESP array nested too deeply");
    }

    nesting++;
    try {
      var len = await ReadInteger(token).ConfigureAwait(false);
      switch (len) {
        case < 0:
          return RespNullArray.Value;
        case 0:
          return new RespArray();
        case > 1073741824:
          throw new RespProtocolException("RESP array too large");
      }

      var arr = new RespType[len];
      for (var i = 0; i < len; i++) {
        arr[i] = await ReadNextUnlocked(token).ConfigureAwait(false);
      }

      return new RespArray(arr);
    }
    finally {
      nesting--;
    }
  }

  private async Task<RespType> ReadNewBulkString(CancellationToken token)
  {
    var len = await ReadInteger(token).ConfigureAwait(true);
    switch (len) {
      case < 0:
        return RespNullString.Value;
      case > 536870912:
        throw new RespProtocolException("Refusing to read HUGE RESP string");
    }

    var bytes = new byte[len];
    var trail = new byte[2];

    return await stream.ReadAsync(bytes.AsMemory(), token).ConfigureAwait(false) != len ||
           await stream.ReadAsync(trail.AsMemory(), token).ConfigureAwait(false) != 2 ||
           trail[0] != '\r' ||
           trail[1] != '\n'
      ? throw new RespProtocolException("Truncated string")
      : new RespBulkString(bytes);
  }

  internal async Task<RespType> ReadNext(CancellationToken token)
  {
    if (Interlocked.CompareExchange(ref reading, 1, 0) != 0) {
      throw new InvalidOperationException("Already reading");
    }

    try {
      return await ReadNextUnlocked(token).ConfigureAwait(false);
    }
    finally {
      _ = Interlocked.Exchange(ref reading, 0);
    }
  }

  private async Task<RespType> ReadNextUnlocked(CancellationToken token)
  {
    return await ReadByteAsync(token) switch {
      (byte)':' => new RespInteger(await ReadInteger(token)),
      (byte)'+' => new RespString(await ReadSimpleString(token)),
      (byte)'-' => throw new RespResponseException(await ReadSimpleString(token)),
      (byte)'$' => await ReadNewBulkString(token),
      (byte)'*' => await ReadNewArray(token),
      _ => throw new RespProtocolException("Invalid RESP type received")
    };
  }

  private async ValueTask<string> ReadSimpleString(CancellationToken token)
  {
    var buffer = new List<byte>();
    while (true) {
      var c = await ReadByteAsync(token).ConfigureAwait(false);
      switch (c) {
        case (byte)'\r':
          if (await ReadByteAsync(token).ConfigureAwait(false) != '\n') {
            throw new RespProtocolException("Invalid RESP string");
          }

          return Encoding.UTF8.GetString(buffer.ToArray());
        case (byte)'\n':
          throw new RespProtocolException("Invalid RESP string");
        default:
          buffer.Add(c);
          if (buffer.Count > 1048576) {
            throw new RespProtocolException("Very large simple string");
          }

          break;
      }
    }
  }

  internal async Task Write(string value, CancellationToken token)
  {
    await WriteSimple($"+{value}\r\n", token).ConfigureAwait(false);
  }

  internal async Task Write(RespType value, CancellationToken token)
  {
    if (Interlocked.CompareExchange(ref writing, 1, 0) == 1) {
      throw new InvalidOperationException(
        "Cannot write multiple things at the same time");
    }

    try {
      await WriteUnlocked(value, token).ConfigureAwait(false);
    }
    finally {
      _ = Interlocked.Exchange(ref writing, 0);
    }
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task WriteArrayUnlocked(RespArray array, CancellationToken token)
  {
    var header = Encoding.UTF8.GetBytes(
      $"*{array.Elements.Length.ToString(CultureInfo.InvariantCulture)}\r\n");
    await stream.WriteAsync(header.AsMemory(0, header.Length), token)
      .ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(true);

    foreach (var e in array.Elements) {
      await WriteUnlocked(e, token);
    }
  }

  private async Task WriteSimple(string s, CancellationToken token)
  {
    if (Interlocked.CompareExchange(ref writing, 1, 0) == 1) {
      throw new InvalidOperationException(
        "Cannot write multiple things at the same time");
    }

    try {
      await WriteSimpleUnlocked(s, token).ConfigureAwait(false);
    }
    finally {
      _ = Interlocked.Exchange(ref writing, 0);
    }
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task WriteSimpleUnlocked(string s, CancellationToken token)
  {
    var bytes = Encoding.UTF8.GetBytes(s);
    await stream.WriteAsync(bytes.AsMemory(0, bytes.Length), token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(true);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task WriteUnlocked(RespType value, CancellationToken token)
  {
    await (value switch {
      RespArray respArray => WriteArrayUnlocked(respArray, token),
      RespBulkString respBulkString => WriteUnlocked(respBulkString.Value, token),
      RespInteger respInteger => WriteSimpleUnlocked(
        $":{respInteger.Value.ToString(CultureInfo.InvariantCulture)}\r\n",
        token),
      RespString respString => WriteSimpleUnlocked($"+{respString.Value}\r\n", token),
      RespErrorString respErrorString => WriteSimpleUnlocked(
        $"-{respErrorString.Value}\r\n",
        token),
      RespNullArray => WriteSimpleUnlocked("*-1\r\n", token),
      RespNullString => WriteSimpleUnlocked("$-1\r\n", token),
      _ => throw new InvalidOperationException("Invalid resp array")
    }).ConfigureAwait(false);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task WriteUnlocked(byte[] bytes, CancellationToken token)
  {
    var header = Encoding.UTF8.GetBytes(
      $"${bytes.Length.ToString(CultureInfo.InvariantCulture)}\r\n");
    await stream.WriteAsync(header.AsMemory(0, header.Length), token)
      .ConfigureAwait(false);
    await stream.WriteAsync(bytes.AsMemory(0, bytes.Length), token).ConfigureAwait(false);
    await stream.WriteAsync(trailer.AsMemory(0, trailer.Length), token)
      .ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(true);
  }
}

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NMaier.PlaneDB;

internal static class StreamExtensions
{
  private static readonly ArrayPool<byte> intPool =
    ArrayPool<byte>.Create(sizeof(int) + 1, 128);

  private static readonly ArrayPool<byte> pool = ArrayPool<byte>.Create();

  private static readonly byte[] wireFalse = [
    (byte)WireType.Bool,
    0x0
  ];

  private static readonly byte[] wireTrue = [
    (byte)WireType.Bool,
    0x1
  ];

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static Task<byte[]?> ReadArray(this Stream stream, CancellationToken token)
  {
    return ReadArray(stream, -1, token);
  }

  internal static async Task<byte[]?> ReadArray(
    this Stream stream,
    int maxLen,
    CancellationToken token)
  {
    var b = intPool.Rent(sizeof(int));
    await stream.ReadFullAsync(b.AsMemory(0, 1), token).ConfigureAwait(false);
    try {
      switch ((WireType)b[0]) {
        case WireType.Array:
          break;
        case WireType.Bool:
        case WireType.Int:
          throw new IOException("Expected array");
        case WireType.Error:
          await ReadException(stream, token).ConfigureAwait(false);

          return null;
        default:
          throw new IOException("Invalid type");
      }

      await stream.ReadFullAsync(b.AsMemory(0, sizeof(int)), token).ConfigureAwait(false);

      var length = BinaryPrimitives.ReadInt32LittleEndian(b);

      switch (length) {
        case -1:
          return null;
        case < 0:
          throw new IOException("Invalid length");
        case 0:
          return [];
        default:
          if (maxLen >= 0 && length > maxLen) {
            throw new IOException(
              $"Announced array is too large, announced as containing {length:N0} bytes");
          }

          var buffer = new byte[length];
          await stream.ReadFullAsync(buffer.AsMemory(0, length), token)
            .ConfigureAwait(false);

          return buffer;
      }
    }
    finally {
      intPool.Return(b);
    }
  }

  internal static async ValueTask<bool> ReadBool(
    this Stream stream,
    CancellationToken token)
  {
    var b = pool.Rent(1);
    try {
      await stream.ReadFullAsync(b.AsMemory(0, 1), token).ConfigureAwait(false);
      switch ((WireType)b[0]) {
        case WireType.Bool:
          await stream.ReadFullAsync(b.AsMemory(0, 1), token).ConfigureAwait(true);

          return b[0] != 0;
        case WireType.Int:
        case WireType.Array:
          throw new IOException("Expected bool");
        case WireType.Error:
          await ReadException(stream, token).ConfigureAwait(false);

          return false;
        default:
          throw new IOException("Invalid type");
      }
    }
    finally {
      pool.Return(b);
    }
  }

  private static async Task ReadException(this Stream stream, CancellationToken token)
  {
    const int BUF_LEN = 5;
    var b = intPool.Rent(BUF_LEN);
    try {
      await stream.ReadFullAsync(b.AsMemory(0, BUF_LEN), token).ConfigureAwait(false);

      var length = BinaryPrimitives.ReadInt32LittleEndian(b.AsSpan(1));

      var message = length switch {
        0 => string.Empty,
        > 0 => await ReadMessage().ConfigureAwait(false),
        _ => throw new IOException("Invalid error")
      };

      throw (ExceptionType)b[0] switch {
        ExceptionType.General => new Exception(message),
        ExceptionType.IO => new IOException(message),
        ExceptionType.ReadOnly => new PlaneDBReadOnlyException(message),
        ExceptionType.NotSupported => new NotSupportedException(message),
        ExceptionType.NotImplemented => new NotImplementedException(message),
        _ => new IOException("Invalid error")
      };

      async Task<string> ReadMessage()
      {
        var messageBuffer = pool.Rent(length);
        try {
          await stream.ReadFullAsync(messageBuffer.AsMemory(0, length), token)
            .ConfigureAwait(false);

          return Encoding.UTF8.GetString(messageBuffer);
        }
        finally {
          pool.Return(messageBuffer);
        }
      }
    }
    finally {
      intPool.Return(b);
    }
  }

  private static async Task ReadFullAsync(
    this Stream stream,
    Memory<byte> buffer,
    CancellationToken token)
  {
    var remaining = buffer.Length;
    var totalRead = 0;
    for (;;) {
      var read = await stream.ReadAsync(buffer, token).ConfigureAwait(false);
      if (read <= 0) {
        throw new IOException(
          $"Truncated read; expected {remaining} bytes but got {totalRead} bytes");
      }

      remaining -= read;
      if (remaining <= 0) {
        return;
      }

      totalRead += read;
      buffer = buffer[read..];
    }
  }

  internal static async ValueTask<int> ReadInt(
    this Stream stream,
    CancellationToken token)
  {
    var b = intPool.Rent(sizeof(int));
    try {
      await stream.ReadFullAsync(b.AsMemory(0, 1), token).ConfigureAwait(false);
      switch ((WireType)b[0]) {
        case WireType.Int:
          break;
        case WireType.Bool:
        case WireType.Array:
          throw new IOException("Expected int");
        case WireType.Error:
          await ReadException(stream, token).ConfigureAwait(false);

          return 0;
        default:
          throw new IOException("Invalid type");
      }

      await stream.ReadFullAsync(b.AsMemory(0, sizeof(int)), token).ConfigureAwait(false);

      return BinaryPrimitives.ReadInt32LittleEndian(b);
    }
    finally {
      intPool.Return(b);
    }
  }

  internal static async Task WriteArray(
    this Stream stream,
    byte[] bytes,
    CancellationToken token)
  {
    const int BUF_LEN = 1 + sizeof(int);
    var buf = intPool.Rent(BUF_LEN);
    try {
      buf[0] = (byte)WireType.Array;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), bytes.Length);
      await stream.WriteAsync(buf.AsMemory(0, BUF_LEN), token).ConfigureAwait(false);
      if (bytes.Length > 0) {
        await stream.WriteAsync(bytes, token).ConfigureAwait(false);
      }
    }
    finally {
      pool.Return(buf);
    }
  }

  internal static async Task WriteBool(
    this Stream stream,
    bool boolean,
    CancellationToken token)
  {
    if (boolean) {
      await stream.WriteAsync(wireTrue, token).ConfigureAwait(false);
    }
    else {
      await stream.WriteAsync(wireFalse, token).ConfigureAwait(false);
    }
  }

  internal static async Task WriteCommand(
    this Stream stream,
    CommandCode value,
    PlaneProtocolRandom canarySource,
    CancellationToken token)
  {
    const int BUF_SIZE = (sizeof(int) * 2) + 2;
    var canary = canarySource.Next();
    var buf = pool.Rent(BUF_SIZE);
    try {
      buf[0] = (byte)WireType.Int;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), canary);
      buf[5] = (byte)WireType.Int;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(6), (int)value);
      await stream.WriteAsync(buf.AsMemory(0, BUF_SIZE), token).ConfigureAwait(false);
    }
    finally {
      pool.Return(buf);
    }
  }

  internal static async Task WriteException(
    this Stream stream,
    Exception value,
    CancellationToken token)
  {
    var message = string.IsNullOrEmpty(value.Message)
      ? []
      : Encoding.UTF8.GetBytes(value.Message);
    var excType = value switch {
      PlaneDBReadOnlyException => ExceptionType.ReadOnly,
      NotSupportedException => ExceptionType.NotSupported,
      NotImplementedException => ExceptionType.NotImplemented,
      IOException => ExceptionType.IO,
      _ => ExceptionType.General
    };
    var len = sizeof(int) + 2 + message.Length;
    var buf = pool.Rent(len);
    try {
      buf[0] = (byte)WireType.Error;
      buf[1] = (byte)excType;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(2), message.Length);
      if (message.Length > 0) {
        message.AsSpan().CopyTo(buf.AsSpan(2 + sizeof(int)));
      }

      await stream.WriteAsync(buf.AsMemory(0, len), token).ConfigureAwait(false);
    }
    finally {
      pool.Return(buf);
    }
  }

  internal static async Task WriteInt(
    this Stream stream,
    int value,
    CancellationToken token)
  {
    var buf = intPool.Rent(sizeof(int) + 1);
    try {
      buf[0] = (byte)WireType.Int;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), value);
      await stream.WriteAsync(buf.AsMemory(0, sizeof(int) + 1), token)
        .ConfigureAwait(false);
    }
    finally {
      intPool.Return(buf);
    }
  }

  internal static async Task WriteMissing(this Stream stream, CancellationToken token)
  {
    var buf = intPool.Rent(sizeof(int) + 1);
    try {
      buf[0] = (byte)WireType.Array;
      BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(1), -1);
      await stream.WriteAsync(buf.AsMemory(0, sizeof(int) + 1), token)
        .ConfigureAwait(false);
    }
    finally {
      intPool.Return(buf);
    }
  }

  private enum ExceptionType : byte
  {
    General = 1,
    IO,
    ReadOnly,
    NotSupported,
    NotImplemented
  }

  private enum WireType : byte
  {
    Bool = 1,
    Int,
    Array,
    Error
  }
}

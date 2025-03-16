using System;
using System.Diagnostics.Contracts;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

using NMaier.BlockStream;

using static System.Buffers.Binary.BinaryPrimitives;

namespace NMaier.PlaneDB;

internal static class InternalExtensions
{
  [MethodImpl(Constants.SHORT_METHOD)]
  internal static IDisposable AcquireReadLock(this IPlaneReadWriteLock rwLock)
  {
    return new ReadLocker(rwLock);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static IDisposable AcquireUpgradableLock(this IPlaneReadWriteLock rwLock)
  {
    return new UpgradeableLocker(rwLock);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static IDisposable AcquireWriteLock(this IPlaneReadWriteLock rwLock)
  {
    return new WriteLocker(rwLock);
  }

  [Pure]
  [MethodImpl(Constants.HOT_METHOD)]
  internal static int ComputeXXHash(this ReadOnlySpan<byte> bytes, uint seed = 0)
  {
    const uint PRIME32_1 = 2654435761U;
    const uint PRIME32_2 = 2246822519U;
    const uint PRIME32_3 = 3266489917U;
    const uint PRIME32_4 = 668265263U;
    const uint PRIME32_5 = 374761393U;

    uint h32;
    var remaining = bytes.Length;
    var total = remaining;

    if (remaining >= 16) {
      var v1 = seed + PRIME32_1 + PRIME32_2;
      var v2 = seed + PRIME32_2;
      var v3 = seed + 0;
      var v4 = seed - PRIME32_1;
      do {
        var nval = ReadUInt32LittleEndian(bytes);
        v1 += nval * PRIME32_2;
        v1 = (v1 << 13) | (v1 >> 19);
        v1 *= PRIME32_1;
        bytes = bytes[sizeof(uint)..];

        var nval1 = ReadUInt32LittleEndian(bytes);
        v2 += nval1 * PRIME32_2;
        v2 = (v2 << 13) | (v2 >> 19);
        v2 *= PRIME32_1;
        bytes = bytes[sizeof(uint)..];

        var nval2 = ReadUInt32LittleEndian(bytes);
        v3 += nval2 * PRIME32_2;
        v3 = (v3 << 13) | (v3 >> 19);
        v3 *= PRIME32_1;
        bytes = bytes[sizeof(uint)..];

        var nval3 = ReadUInt32LittleEndian(bytes);
        v4 += nval3 * PRIME32_2;
        v4 = (v4 << 13) | (v4 >> 19);
        v4 *= PRIME32_1;
        bytes = bytes[sizeof(uint)..];
        remaining -= 16;
      } while (remaining >= 16);

      h32 = ((v1 << 1) | (v1 >> 31)) +
            ((v2 << 7) | (v2 >> 25)) +
            ((v3 << 12) | (v3 >> 20)) +
            ((v4 << 18) | (v4 >> 14));
    }
    else {
      h32 = seed + PRIME32_5;
    }

    h32 += unchecked((uint)total);

    while (remaining >= sizeof(uint)) {
      h32 += ReadUInt32LittleEndian(bytes) * PRIME32_3;
      h32 = ((h32 << 17) | (h32 >> 15)) * PRIME32_4;
      bytes = bytes[sizeof(uint)..];
      remaining -= sizeof(uint);
    }

    for (var i = 0; i < remaining; ++i) {
      h32 += bytes[i] * PRIME32_5;
      h32 = ((h32 << 11) | (h32 >> 21)) * PRIME32_1;
    }

    h32 ^= h32 >> 15;
    h32 *= PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= PRIME32_3;
    h32 ^= h32 >> 16;

    return unchecked((int)h32);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static bool ContainsKey(this IReadableTable table, string key)
  {
    var keyBytes = Encoding.UTF8.GetBytes(key);

    return table.ContainsKey(keyBytes, new BloomFilter.Hashes(keyBytes), out _);
  }

  internal static void CopyTo(this IReadableTable from, IWritableTable to)
  {
    foreach (var (key, value) in from.Enumerate()) {
      if (value == null) {
        to.Remove(key);
      }
      else {
        to.Put(key, value);
      }
    }
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static void Put(this IWritableTable table, string key, string val)
  {
    table.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(val));
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static byte[] ReadFullBlock(this Stream stream, int length)
  {
    var buffer = new byte[length];
    stream.ReadFullBlock(buffer);

    return buffer;
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static int ReadInt32(this Stream stream)
  {
    Span<byte> buffer = stackalloc byte[sizeof(int)];
    stream.ReadFullBlock(buffer, sizeof(int));

    return ReadInt32LittleEndian(buffer);
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static long ReadInt64(this Stream stream)
  {
    Span<byte> buffer = stackalloc byte[sizeof(long)];
    stream.ReadFullBlock(buffer, sizeof(long));

    return ReadInt64LittleEndian(buffer);
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static ulong ReadUInt64(this Stream stream)
  {
    Span<byte> buffer = stackalloc byte[sizeof(ulong)];
    stream.ReadFullBlock(buffer, sizeof(ulong));

    return ReadUInt64LittleEndian(buffer);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  internal static void Remove(this IWritableTable table, string key)
  {
    table.Remove(Encoding.UTF8.GetBytes(key));
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static bool TryGet(this IReadableTable table, string key, out string? value)
  {
    var keyBytes = Encoding.UTF8.GetBytes(key);
    if (!table.TryGet(keyBytes, new BloomFilter.Hashes(keyBytes), out var raw)) {
      value = null;

      return false;
    }

    value = raw == null ? null : Encoding.UTF8.GetString(raw);

    return true;
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static void WriteInt32(this Stream stream, int value)
  {
    Span<byte> b = stackalloc byte[sizeof(int)];
    WriteInt32LittleEndian(b, value);
    stream.Write(b);
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static void WriteInt64(this Stream stream, long value)
  {
    Span<byte> b = stackalloc byte[sizeof(long)];
    WriteInt64LittleEndian(b, value);
    stream.Write(b);
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  internal static void WriteUInt64(this Stream stream, ulong value)
  {
    Span<byte> b = stackalloc byte[sizeof(ulong)];
    WriteUInt64LittleEndian(b, value);
    stream.Write(b);
  }

  private readonly struct ReadLocker : IDisposable
  {
    private readonly IPlaneReadWriteLock rwLock;
    private readonly bool taken;

    [MethodImpl(Constants.SHORT_METHOD)]
    public ReadLocker(IPlaneReadWriteLock rwLock)
    {
      this.rwLock = rwLock;
      rwLock.EnterReadLock(out taken);
    }

    [MethodImpl(Constants.SHORT_METHOD)]
    public void Dispose()
    {
      if (taken) {
        rwLock.ExitReadLock();
      }
    }
  }

  private readonly struct UpgradeableLocker : IDisposable
  {
    private readonly IPlaneReadWriteLock rwLock;
    private readonly bool taken;

    [MethodImpl(Constants.SHORT_METHOD)]
    public UpgradeableLocker(IPlaneReadWriteLock rwLock)
    {
      this.rwLock = rwLock;
      rwLock.EnterUpgradeableReadLock(out taken);
    }

    [MethodImpl(Constants.SHORT_METHOD)]
    public void Dispose()
    {
      if (taken) {
        rwLock.ExitUpgradeableReadLock();
      }
    }
  }

  private readonly struct WriteLocker : IDisposable
  {
    private readonly IPlaneReadWriteLock rwLock;
    private readonly bool taken;

    [MethodImpl(Constants.SHORT_METHOD)]
    public WriteLocker(IPlaneReadWriteLock rwLock)
    {
      this.rwLock = rwLock;
      rwLock.EnterWriteLock(out taken);
    }

    [MethodImpl(Constants.SHORT_METHOD)]
    public void Dispose()
    {
      if (taken) {
        rwLock.ExitWriteLock();
      }
    }
  }
}

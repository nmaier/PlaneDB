using System;
using System.IO;
using System.Text;
using JetBrains.Annotations;
using NMaier.BlockStream;
using static System.Buffers.Binary.BinaryPrimitives;

namespace NMaier.PlaneDB
{
  /// <summary>
  ///   Various helpful type extensions
  /// </summary>
  [PublicAPI]
  public static class Extensions
  {
    /// <summary>
    ///   Computes an murmur3 for this sequence of bytes
    /// </summary>
    /// <param name="bytes">Input bytes</param>
    /// <param name="seed">Hash seed</param>
    /// <returns>Computed murmur3</returns>
    public static int ComputeMurmur3(this ReadOnlySpan<byte> bytes, uint seed)
    {
      const uint C1 = 0xcc9e2d51;
      const uint C2 = 0x1b873593;
      var h1 = seed;

      int index;
      for (index = 0; index < bytes.Length - 4; index += 4) {
        var slice = bytes.Slice(index, 4);
        /* Get four bytes from the input into an uint */
        var k1 = (uint)
          (slice[0]
           | (slice[1] << 8)
           | (slice[2] << 16)
           | (slice[3] << 24));

        /* bitmagic hash */
        k1 *= C1;
        k1 = Rotl32(k1, 15);
        k1 *= C2;

        h1 ^= k1;
        h1 = Rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
      }

      // handle remainder
      if (index < bytes.Length) {
        var slice = bytes.Slice(index, bytes.Length - index);
        uint k1;
        // ReSharper disable once SwitchStatementMissingSomeCases
        switch (slice.Length) {
          case 3:
            k1 = (uint)
              (slice[0]
               | (slice[1] << 8)
               | (slice[2] << 16));
            k1 *= C1;
            k1 = Rotl32(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            break;
          case 2:
            k1 = (uint)
              (slice[0]
               | (slice[1] << 8));
            k1 *= C1;
            k1 = Rotl32(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            break;
          case 1:
            k1 = slice[0];
            k1 *= C1;
            k1 = Rotl32(k1, 15);
            k1 *= C2;
            h1 ^= k1;
            break;
        }
      }

      // finalization, magic chants to wrap it all up
      h1 ^= (uint)bytes.Length;
      h1 = Fmix(h1);

      unchecked //ignore overflow
      {
        return (int)h1;
      }
    }

    /// <summary>
    ///   Computes an xxHash for this sequence of bytes
    /// </summary>
    /// <param name="bytes">Input bytes</param>
    /// <param name="seed">Hash seed</param>
    /// <returns>Computed xxHash</returns>
    public static int ComputeXXHash(this ReadOnlySpan<byte> bytes, uint seed = 0)
    {
      const uint PRIME32_1 = 2654435761U;
      const uint PRIME32_2 = 2246822519U;
      const uint PRIME32_3 = 3266489917U;
      const uint PRIME32_4 = 668265263U;
      const uint PRIME32_5 = 374761393U;

      uint h32;
      var len = bytes.Length;

      if (bytes.Length >= 16) {
        uint v1 = seed + PRIME32_1 + PRIME32_2;
        uint v2 = seed + PRIME32_2;
        uint v3 = seed + 0;
        uint v4 = seed - PRIME32_1;
        do {
          var nval = ReadUInt32LittleEndian(bytes);
          v1 += nval * PRIME32_2;
          v1 = (v1 << 13) | (v1 >> 19);
          v1 *= PRIME32_1;
          bytes = bytes.Slice(sizeof(uint));

          var nval1 = ReadUInt32LittleEndian(bytes);
          v2 += nval1 * PRIME32_2;
          v2 = (v2 << 13) | (v2 >> 19);
          v2 *= PRIME32_1;
          bytes = bytes.Slice(sizeof(uint));

          var nval2 = ReadUInt32LittleEndian(bytes);
          v3 += nval2 * PRIME32_2;
          v3 = (v3 << 13) | (v3 >> 19);
          v3 *= PRIME32_1;
          bytes = bytes.Slice(sizeof(uint));

          var nval3 = ReadUInt32LittleEndian(bytes);
          v4 += nval3 * PRIME32_2;
          v4 = (v4 << 13) | (v4 >> 19);
          v4 *= PRIME32_1;
          bytes = bytes.Slice(sizeof(uint));
        } while (bytes.Length >= 16);

        h32 = ((v1 << 1) | (v1 >> 31)) + ((v2 << 7) | (v2 >> 25)) + ((v3 << 12) | (v3 >> 20)) +
              ((v4 << 18) | (v4 >> 14));
      }
      else {
        h32 = seed + PRIME32_5;
      }

      h32 += unchecked((uint)len);

      while (bytes.Length >= sizeof(uint)) {
        h32 += ReadUInt32LittleEndian(bytes) * PRIME32_3;
        h32 = ((h32 << 17) | (h32 >> 15)) * PRIME32_4;
        bytes = bytes.Slice(sizeof(uint));
      }

      for (int i = 0, e = bytes.Length; i < e; ++i) {
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

    internal static bool ContainsKey(this IReadOnlyTable table, string key, out bool removed)
    {
      return table.ContainsKey(Encoding.UTF8.GetBytes(key), out removed);
    }

    internal static void Put(this IWriteOnlyTable table, string key, string val)
    {
      table.Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(val));
    }

    internal static int ReadInt32(this Stream stream)
    {
      return ReadInt32LittleEndian(stream.ReadFullBlock(sizeof(int)));
    }

    internal static long ReadInt64(this Stream stream)
    {
      return ReadInt64LittleEndian(stream.ReadFullBlock(sizeof(long)));
    }

    internal static uint ReadUInt32(this Stream stream)
    {
      return ReadUInt32LittleEndian(stream.ReadFullBlock(sizeof(uint)));
    }

    internal static ulong ReadUInt64(this Stream stream)
    {
      return ReadUInt64LittleEndian(stream.ReadFullBlock(sizeof(ulong)));
    }

    internal static byte[] ReadFullBlock(this Stream stream, int length)
    {
      var buffer = new byte[length];
      stream.ReadFullBlock(buffer);
      return buffer;
    }

#if NET48
    internal static bool Remove<TKey, TValue>(this System.Collections.Generic.IDictionary<TKey, TValue> dictionary, TKey key, out TValue value)
    {
      return dictionary.TryGetValue(key, out value) && dictionary.Remove(key);
    }
#endif

    internal static bool Remove(this IWriteOnlyTable table, string key)
    {
      return table.Remove(Encoding.UTF8.GetBytes(key));
    }

    internal static bool TryGet(this IReadOnlyTable table, string key, out string? value)
    {
      if (!table.TryGet(Encoding.UTF8.GetBytes(key), out var raw)) {
        value = default;
        return false;
      }

      value = raw == null ? null : Encoding.UTF8.GetString(raw);
      return true;
    }

    internal static bool Update(this IWriteOnlyTable table, string key, string val)
    {
      return table.Update(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(val));
    }

#if NET48
    internal static void Write(this Stream stream, ReadOnlySpan<byte> bytes)
    {
      var copy = System.Buffers.ArrayPool<byte>.Shared.Rent(bytes.Length);
      try {
        bytes.CopyTo(copy);
        stream.Write(copy, 0, bytes.Length);
      }
      finally {
        System.Buffers.ArrayPool<byte>.Shared.Return(copy);
      }
    }
#endif

    internal static void WriteInt32(this Stream stream, int value)
    {
      Span<byte> b = stackalloc byte[sizeof(int)];
      WriteInt32LittleEndian(b, value);
      stream.Write(b);
    }

    internal static void WriteInt32(this Stream stream, uint value)
    {
      Span<byte> b = stackalloc byte[sizeof(uint)];
      WriteUInt32LittleEndian(b, value);
      stream.Write(b);
    }

    internal static void WriteInt64(this Stream stream, long value)
    {
      Span<byte> b = stackalloc byte[sizeof(long)];
      WriteInt64LittleEndian(b, value);
      stream.Write(b);
    }

    internal static void WriteUInt64(this Stream stream, ulong value)
    {
      Span<byte> b = stackalloc byte[sizeof(ulong)];
      WriteUInt64LittleEndian(b, value);
      stream.Write(b);
    }

    private static uint Fmix(uint h)
    {
      h ^= h >> 16;
      h *= 0x85ebca6b;
      h ^= h >> 13;
      h *= 0xc2b2ae35;
      h ^= h >> 16;
      return h;
    }

    private static uint Rotl32(uint x, byte r) => (x << r) | (x >> (32 - r));
  }
}
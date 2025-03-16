using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneDecimalSerializer : IPlaneSerializer<decimal>
{
  /// <inheritdoc />
  public decimal Deserialize(ReadOnlySpan<byte> bytes)
  {
    int[] bits = [
      BinaryPrimitives.ReadInt32LittleEndian(bytes),
      BinaryPrimitives.ReadInt32LittleEndian(bytes[4..]),
      BinaryPrimitives.ReadInt32LittleEndian(bytes[8..]),
      BinaryPrimitives.ReadInt32LittleEndian(bytes[12..])
    ];

    return new decimal(bits);
  }

  /// <inheritdoc />
  public byte[] Serialize(in decimal obj)
  {
    Span<byte> bytes = stackalloc byte[sizeof(int) * 4];
    Span<int> integers = stackalloc int[4];
    _ = decimal.TryGetBits(obj, integers, out _);

    BinaryPrimitives.WriteInt32LittleEndian(bytes, integers[0]);
    BinaryPrimitives.WriteInt32LittleEndian(bytes[4..], integers[1]);
    BinaryPrimitives.WriteInt32LittleEndian(bytes[8..], integers[2]);
    BinaryPrimitives.WriteInt32LittleEndian(bytes[12..], integers[3]);

    return bytes.ToArray();
  }
}

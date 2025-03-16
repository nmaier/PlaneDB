using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneDateTimeSerializer : IPlaneSerializer<DateTime>
{
  /// <inheritdoc />
  public DateTime Deserialize(ReadOnlySpan<byte> bytes)
  {
    return DateTime.FromBinary(BinaryPrimitives.ReadInt64LittleEndian(bytes));
  }

  /// <inheritdoc />
  public byte[] Serialize(in DateTime obj)
  {
    var rv = new byte[8];
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(), obj.ToBinary());

    return rv;
  }
}

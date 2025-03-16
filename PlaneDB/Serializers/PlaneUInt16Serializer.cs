using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneUInt16Serializer : IPlaneSerializer<ushort>
{
  /// <inheritdoc />
  public ushort Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadUInt16LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in ushort obj)
  {
    var rv = new byte[sizeof(ushort)];
    BinaryPrimitives.WriteUInt16LittleEndian(rv, obj);

    return rv;
  }
}

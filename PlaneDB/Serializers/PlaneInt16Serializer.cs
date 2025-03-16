using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneInt16Serializer : IPlaneSerializer<short>
{
  /// <inheritdoc />
  public short Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadInt16LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in short obj)
  {
    var rv = new byte[sizeof(short)];
    BinaryPrimitives.WriteInt16LittleEndian(rv, obj);

    return rv;
  }
}

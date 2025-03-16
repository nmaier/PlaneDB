using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneUInt32Serializer : IPlaneSerializer<uint>
{
  /// <inheritdoc />
  public uint Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in uint obj)
  {
    var rv = new byte[sizeof(uint)];
    BinaryPrimitives.WriteUInt32LittleEndian(rv, obj);

    return rv;
  }
}

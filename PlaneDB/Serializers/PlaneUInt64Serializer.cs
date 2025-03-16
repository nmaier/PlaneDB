using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneUInt64Serializer : IPlaneSerializer<ulong>
{
  /// <inheritdoc />
  public ulong Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadUInt64LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in ulong obj)
  {
    var rv = new byte[sizeof(ulong)];
    BinaryPrimitives.WriteUInt64LittleEndian(rv, obj);

    return rv;
  }
}

using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneInt64Serializer : IPlaneSerializer<long>
{
  /// <inheritdoc />
  public long Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadInt64LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in long obj)
  {
    var rv = new byte[sizeof(long)];
    BinaryPrimitives.WriteInt64LittleEndian(rv, obj);

    return rv;
  }
}

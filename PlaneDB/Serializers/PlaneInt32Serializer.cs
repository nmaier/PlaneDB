using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneInt32Serializer : IPlaneSerializer<int>
{
  /// <inheritdoc />
  public int Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadInt32LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in int obj)
  {
    var rv = new byte[sizeof(int)];
    BinaryPrimitives.WriteInt32LittleEndian(rv, obj);

    return rv;
  }
}

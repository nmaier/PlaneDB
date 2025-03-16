using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneFloatSerializer : IPlaneSerializer<float>
{
  /// <inheritdoc />
  public float Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadSingleLittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in float obj)
  {
    var rv = new byte[sizeof(float)];
    BinaryPrimitives.WriteSingleLittleEndian(rv, obj);

    return rv;
  }
}

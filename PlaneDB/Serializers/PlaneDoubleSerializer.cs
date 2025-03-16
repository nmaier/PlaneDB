using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneDoubleSerializer : IPlaneSerializer<double>
{
  /// <inheritdoc />
  public double Deserialize(ReadOnlySpan<byte> bytes)
  {
    return BinaryPrimitives.ReadDoubleLittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in double obj)
  {
    var rv = new byte[sizeof(double)];
    BinaryPrimitives.WriteDoubleLittleEndian(rv, obj);

    return rv;
  }
}

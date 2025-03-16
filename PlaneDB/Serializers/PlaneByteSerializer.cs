using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneByteSerializer : IPlaneSerializer<byte>
{
  /// <inheritdoc />
  public byte Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes[0];
  }

  /// <inheritdoc />
  public byte[] Serialize(in byte obj)
  {
    return [
      obj
    ];
  }
}

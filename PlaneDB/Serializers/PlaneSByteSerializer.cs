using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneSByteSerializer : IPlaneSerializer<sbyte>
{
  /// <inheritdoc />
  public sbyte Deserialize(ReadOnlySpan<byte> bytes)
  {
    return unchecked((sbyte)bytes[0]);
  }

  /// <inheritdoc />
  public byte[] Serialize(in sbyte obj)
  {
    return [
      unchecked((byte)obj)
    ];
  }
}

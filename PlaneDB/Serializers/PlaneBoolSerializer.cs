using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneBoolSerializer : IPlaneSerializer<bool>
{
  /// <inheritdoc />
  public bool Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes[0] != 0;
  }

  /// <inheritdoc />
  public byte[] Serialize(in bool obj)
  {
    return [
      (byte)(obj ? 0x1 : 0x0)
    ];
  }
}

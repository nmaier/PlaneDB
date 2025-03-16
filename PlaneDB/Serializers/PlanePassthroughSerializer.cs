using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public class PlanePassthroughSerializer : IPlaneSerializer<byte[]>
{
  /// <inheritdoc />
  public byte[] Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes.ToArray();
  }

  /// <inheritdoc />
  public byte[] Serialize(in byte[] obj)
  {
    return obj;
  }
}

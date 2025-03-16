using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneGuidSerializer : IPlaneSerializer<Guid>
{
  /// <inheritdoc />
  public Guid Deserialize(ReadOnlySpan<byte> bytes)
  {
    return new Guid(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in Guid obj)
  {
    return obj.ToByteArray();
  }
}

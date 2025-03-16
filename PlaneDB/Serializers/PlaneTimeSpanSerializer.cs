using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneTimeSpanSerializer : IPlaneSerializer<TimeSpan>
{
  /// <inheritdoc />
  public TimeSpan Deserialize(ReadOnlySpan<byte> bytes)
  {
    return TimeSpan.FromTicks(BinaryPrimitives.ReadInt64LittleEndian(bytes));
  }

  /// <inheritdoc />
  public byte[] Serialize(in TimeSpan obj)
  {
    var rv = new byte[sizeof(long)];
    BinaryPrimitives.WriteInt64LittleEndian(rv, obj.Ticks);

    return rv;
  }
}

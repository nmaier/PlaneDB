using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Nullable plain-old-data type serializer
/// </summary>
/// <param name="underlying">Underlying element serializer</param>
[PublicAPI]
public sealed class PlaneNullablePlainSerializer<T>(IPlaneSerializer<T> underlying)
  : IPlaneSerializer<T?> where T : struct
{
  /// <inheritdoc />
  public T? Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes[0] == 0 ? null : underlying.Deserialize(bytes[1..]);
  }

  /// <inheritdoc />
  public byte[] Serialize(in T? obj)
  {
    if (!obj.HasValue) {
      return [
        0
      ];
    }

    var val = underlying.Serialize(obj.Value);
    var rv = new byte[val.Length + 1];
    rv[0] = 1;
    val.AsSpan().CopyTo(rv.AsSpan(1));

    return rv;
  }
}

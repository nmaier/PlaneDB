using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Nullable objects
/// </summary>
/// <param name="underlying">Underlying non-nullable object type</param>
[PublicAPI]
public sealed class PlaneNullableObjectSerializer<T>(IPlaneSerializer<T> underlying)
  : IPlaneSerializer<T?> where T : class
{
  /// <inheritdoc />
  public T? Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes[0] == 0 ? null : underlying.Deserialize(bytes[1..]);
  }

  /// <inheritdoc />
  public byte[] Serialize(in T? obj)
  {
    if (obj == null) {
      return [
        0
      ];
    }

    var val = underlying.Serialize(obj);
    var rv = new byte[val.Length + 1];
    rv[0] = 1;
    val.AsSpan().CopyTo(rv.AsSpan(1));

    return rv;
  }
}

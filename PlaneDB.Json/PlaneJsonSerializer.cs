using System;
using System.Text.Json;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Serialize objects using MessagePack for storage in a PlaneDB
/// </summary>
/// <typeparam name="T">Object type to be serialized</typeparam>
[PublicAPI]
public sealed class PlaneJsonSerializer<T> : IPlaneSerializer<T>
{
  /// <inheritdoc />
  public T Deserialize(ReadOnlySpan<byte> bytes)
  {
    return JsonSerializer.Deserialize<T>(bytes) ??
           throw new ArgumentException("Not valid json", nameof(bytes));
  }

  /// <inheritdoc />
  public byte[] Serialize(in T obj)
  {
    return JsonSerializer.SerializeToUtf8Bytes(obj);
  }
}

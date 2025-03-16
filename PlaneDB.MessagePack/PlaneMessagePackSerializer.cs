using System;
using System.IO;

using JetBrains.Annotations;

using MessagePack;

namespace NMaier.PlaneDB;

/// <summary>
///   Serialize objects using MessagePack for storage in a PlaneDB
/// </summary>
/// <typeparam name="T">Object type to be serialized</typeparam>
[PublicAPI]
public sealed class PlaneMessagePackSerializer<T> : IPlaneSerializer<T>
{
  /// <inheritdoc />
  public T Deserialize(ReadOnlySpan<byte> bytes)
  {
    using var ms = new MemoryStream(bytes.ToArray(), 0, bytes.Length, false);

    return MessagePackSerializer.Deserialize<T>(ms);
  }

  /// <inheritdoc />
  public byte[] Serialize(in T obj)
  {
    return MessagePackSerializer.Serialize(obj);
  }
}

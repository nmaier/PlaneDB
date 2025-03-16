using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Minimal serialization interface for typed PlaneDBs/Sets
/// </summary>
/// <typeparam name="T">Type to handle</typeparam>
[PublicAPI]
public interface IPlaneSerializer<T>
{
  /// <summary>
  ///   Deserialize an object
  /// </summary>
  /// <param name="bytes">Serialized input</param>
  /// <returns>Deserialized object</returns>
  T Deserialize(ReadOnlySpan<byte> bytes);

  /// <summary>
  ///   Serialize an object
  /// </summary>
  /// <param name="obj">Object to serialize</param>
  /// <returns>Serialized data</returns>
  byte[] Serialize(in T obj);
}

using System;
using System.Runtime.Serialization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>Thrown when a db/set is already locked</summary>
[PublicAPI]
[Serializable]
public sealed class PlaneDBStateException : PlaneDBException
{
  /// <inheritdoc />
  public PlaneDBStateException(string message) : base(message)
  {
  }

  /// <inheritdoc />
  public PlaneDBStateException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }

  /// <inheritdoc />
  [Obsolete("Obsolete")]
  private PlaneDBStateException(
    SerializationInfo serializationInfo,
    StreamingContext streamingContext) : base(serializationInfo, streamingContext)
  {
  }
}

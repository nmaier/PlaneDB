using System;
using System.Runtime.Serialization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Thrown when a db/set cannot be opened because the wrong block transformer is used, or it was really badly
///   corrupted
/// </summary>
[PublicAPI]
[Serializable]
public sealed class PlaneDBBadMagicException : PlaneDBException
{
  /// <inheritdoc />
  public PlaneDBBadMagicException(string message) : base(message)
  {
  }

  /// <inheritdoc />
  public PlaneDBBadMagicException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }

  internal PlaneDBBadMagicException() : base(
    "Bad file magic; Are you using the wrong options?")
  {
  }

  [Obsolete("Obsolete")]
  private PlaneDBBadMagicException(
    SerializationInfo serializationInfo,
    StreamingContext streamingContext) : base(serializationInfo, streamingContext)
  {
  }
}

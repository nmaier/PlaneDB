using System;
using System.IO;
using System.Runtime.Serialization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Base exception type for PlaneDB related exceptions
/// </summary>
[PublicAPI]
public abstract class PlaneDBException : IOException
{
  /// <inheritdoc />
  protected PlaneDBException(string message) : base(message) { }

  /// <inheritdoc />
  protected PlaneDBException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }

  /// <inheritdoc />
  [Obsolete("Obsolete")]
  protected PlaneDBException(
    SerializationInfo serializationInfo,
    StreamingContext streamingContext) : base(serializationInfo, streamingContext)
  {
  }
}

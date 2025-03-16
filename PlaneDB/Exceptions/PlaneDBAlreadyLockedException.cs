using System;
using System.Runtime.Serialization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>Thrown when a db/set is already locked</summary>
[PublicAPI]
[Serializable]
public sealed class PlaneDBAlreadyLockedException : PlaneDBException
{
  private const string MSG =
    "Cannot lock database location; are you trying to open the same tablespace more than once?";

  /// <inheritdoc />
  public PlaneDBAlreadyLockedException() : base(MSG)
  {
  }

  /// <inheritdoc />
  public PlaneDBAlreadyLockedException(string message) : base(message)
  {
  }

  /// <inheritdoc />
  public PlaneDBAlreadyLockedException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }

  internal PlaneDBAlreadyLockedException(Exception innerException) : base(
    MSG,
    innerException)
  {
  }

  /// <inheritdoc />
  [Obsolete("Obsolete")]
  private PlaneDBAlreadyLockedException(
    SerializationInfo serializationInfo,
    StreamingContext streamingContext) : base(serializationInfo, streamingContext)
  {
  }
}

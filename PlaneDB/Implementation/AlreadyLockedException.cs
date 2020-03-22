using System;
using System.IO;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>Thrown when a db/set is already locked</summary>
  [PublicAPI]
  [Serializable]
  public sealed class AlreadyLockedException : IOException
  {
    internal AlreadyLockedException(Exception innerException) : base(
      "Cannot lock database location; are you trying to open the same tablespace more than once?", innerException)
    {
    }

    /// <inheritdoc />
    public AlreadyLockedException()
    {
    }

    /// <inheritdoc />
    public AlreadyLockedException(string message) : base(message)
    {
    }

    /// <inheritdoc />
    public AlreadyLockedException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <inheritdoc />
    private AlreadyLockedException(SerializationInfo serializationInfo, StreamingContext streamingContext)
      : base(serializationInfo, streamingContext)
    {
    }
  }
}
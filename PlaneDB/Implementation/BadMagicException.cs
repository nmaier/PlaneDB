using System;
using System.IO;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   Thrown when a db/set cannot be opened because the wrong block transformer is used, or it was really badly
  ///   corrupted
  /// </summary>
  [PublicAPI]
  [Serializable]
  public sealed class BadMagicException : IOException
  {
    internal BadMagicException() : base("Bad file magic; Are you using the wrong options?") { }

    /// <inheritdoc />
    public BadMagicException(string message) : base(message)
    {
    }

    /// <inheritdoc />
    public BadMagicException(string message, Exception innerException) : base(message, innerException)
    {
    }

    private BadMagicException(SerializationInfo serializationInfo, StreamingContext streamingContext)
      : base(serializationInfo, streamingContext)
    {
    }
  }
}
using System;
using System.IO;
using System.Runtime.Serialization;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   Thrown when a journal file is broken.
  ///   corrupted
  /// </summary>
  [PublicAPI]
  [Serializable]
  public sealed class BrokenJournalException : IOException
  {
    internal BrokenJournalException() : base("Journal file broken") { }

    internal BrokenJournalException(FileStream fs) : base($"Journal file broken: {fs.Name}")
    {
    }

    private BrokenJournalException(SerializationInfo serializationInfo, StreamingContext streamingContext)
      : base(serializationInfo, streamingContext)
    {
    }
  }
}
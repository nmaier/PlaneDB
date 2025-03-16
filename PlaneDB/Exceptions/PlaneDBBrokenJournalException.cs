using System;
using System.IO;
using System.Runtime.Serialization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Thrown when a journal file is broken.
///   corrupted
/// </summary>
[PublicAPI]
[Serializable]
public sealed class PlaneDBBrokenJournalException : PlaneDBException
{
  internal PlaneDBBrokenJournalException() : base("Journal file broken") { }

  internal PlaneDBBrokenJournalException(FileStream fs) : base(
    $"Journal file broken: {fs.Name}")
  {
  }

  [Obsolete("Obsolete")]
  private PlaneDBBrokenJournalException(
    SerializationInfo serializationInfo,
    StreamingContext streamingContext) : base(serializationInfo, streamingContext)
  {
  }
}

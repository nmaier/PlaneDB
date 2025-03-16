using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Thrown when a PlaneDB was opened read-only but an attempt to write to it was made.
/// </summary>
[PublicAPI]
public sealed class PlaneDBReadOnlyException : PlaneDBException
{
  internal PlaneDBReadOnlyException() : base("Database is read-only") { }
  internal PlaneDBReadOnlyException(string message) : base(message) { }
}

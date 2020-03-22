using System.IO;
using JetBrains.Annotations;
using NMaier.Serializers;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   A simple string set store
  /// </summary>
  [PublicAPI]
  public sealed class StringPlaneSet : TypedPlaneSet<string>
  {
    /// <inheritdoc />
    /// <summary>
    ///   Creates a new string persistent set
    /// </summary>
    /// <param name="location">Directory that will store the PlaneSet</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    public StringPlaneSet(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
      : base(new StringSerializer(), location, mode, options)
    {
    }
  }
}
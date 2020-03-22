using System.IO;
using JetBrains.Annotations;
using NMaier.Serializers;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   A simple String/String Key-Value store
  /// </summary>
  [PublicAPI]
  public sealed class StringPlaneDB : TypedPlaneDB<string, string>
  {
    /// <summary>
    ///   Create a new String/String Key-Value store
    /// </summary>
    /// <param name="location">Directory that will store the PlaneDB</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    public StringPlaneDB(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
      : base(new StringSerializer(), new StringSerializer(), location, mode, options)
    {
    }
  }
}
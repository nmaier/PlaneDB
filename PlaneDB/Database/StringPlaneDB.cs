using System.IO;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

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
  /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  public StringPlaneDB(DirectoryInfo location, PlaneOptions options) : base(
    new PlaneStringSerializer(),
    new PlaneStringSerializer(),
    location,
    options)
  {
  }
}

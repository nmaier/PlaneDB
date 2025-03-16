using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Mode for compacting a database
/// </summary>
[PublicAPI]
public enum CompactionMode
{
  /// <summary>
  ///   Normal, will create multiple level files
  /// </summary>
  Normal,

  /// <summary>
  ///   Fully, will create one super level file
  /// </summary>
  Fully
}

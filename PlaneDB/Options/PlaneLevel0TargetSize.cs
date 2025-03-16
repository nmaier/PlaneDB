using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Target size to use.
/// </summary>
[PublicAPI]
public enum PlaneLevel0TargetSize : long
{
  /// <summary>
  ///   DefaultSize size
  /// </summary>
  DefaultSize = Constants.LEVEL10_TARGET_SIZE,

  /// <summary>
  ///   Double the default size
  /// </summary>
  DoubleSize = Constants.LEVEL10_TARGET_SIZE * 2,

  /// <summary>
  ///   Four times the default size
  /// </summary>
  QuadrupleSize = Constants.LEVEL10_TARGET_SIZE * 4
}

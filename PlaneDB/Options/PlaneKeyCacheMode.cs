using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Define how keys (and inlined values) will be cached.
///   Key caching can improve performance, however will require more memory.
///   Database with large keys or a large number of keys may consume a lot of memory when key caching is enabled!
/// </summary>
[PublicAPI]
public enum PlaneKeyCacheMode
{
  /// <summary>
  ///   Keys (and inlined values) will be cached for SSTable files that are considered heuristically small enough
  /// </summary>
  AutoKeyCaching,

  /// <summary>
  ///   Keys will not be cached
  /// </summary>
  NoKeyCaching,

  /// <summary>
  ///   Keys (and inlined values) will be cached for all SSTable files. This might lead to extensive memory usage.
  /// </summary>
  ForceKeyCaching
}

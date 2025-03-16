using System;

using JetBrains.Annotations;

using NMaier.BlockStream.Transformers;

namespace NMaier.PlaneDB;

/// <summary>
///   Represents a transformer that can be salted.
/// </summary>
[PublicAPI]
public interface IPlaneSaltableBlockTransformer : IBlockTransformer
{
  /// <summary>
  ///   Gets the salted transformer
  /// </summary>
  /// <param name="salt"></param>
  /// <returns></returns>
  IBlockTransformer GetTransformerFor(ReadOnlySpan<byte> salt);
}

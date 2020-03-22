using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <summary>
  ///   Kinda like LevelDB, but in C#, and as a Set!
  /// </summary>
  /// <typeparam name="T">Set item type</typeparam>
  [PublicAPI]
  [SuppressMessage("ReSharper", "PossibleInterfaceMemberAmbiguity")]
  public interface IPlaneSet<T> : IPlaneBase, IReadOnlyCollection<T>, ISet<T>, IProducerConsumerCollection<T>
  {
  }
}
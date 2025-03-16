using System.Collections.Concurrent;
using System.Collections.Generic;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Kinda like LevelDB, but in C#, and as a Set!
/// </summary>
/// <typeparam name="T">Set item type</typeparam>
[PublicAPI]
// ReSharper disable once PossibleInterfaceMemberAmbiguity
public interface IPlaneSet<T> : IPlaneBase, IReadOnlyCollection<T>, ISet<T>,
  IProducerConsumerCollection<T>
{
  /// <summary>
  ///   Registers a merge participant
  /// </summary>
  /// <param name="participant">Participant to register</param>
  public void RegisterMergeParticipant(IPlaneSetMergeParticipant<T> participant);

  /// <summary>
  ///   Removes a registered merge participant again
  /// </summary>
  /// <param name="participant">Participant for which to remove registration</param>
  public void UnregisterMergeParticipant(IPlaneSetMergeParticipant<T> participant);
}

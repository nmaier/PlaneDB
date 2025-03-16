using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <typeparam name="TKey">DB key type</typeparam>
/// <typeparam name="TValue">DB value type</typeparam>
/// <summary>
///   Kinda like LevelDB, but in C#!
/// </summary>
[PublicAPI]
public interface IPlaneDB<TKey, TValue> : IPlaneBase, IPlaneDictionary<TKey, TValue>
  where TKey : notnull
{
  /// <summary>
  ///   Raised when flushing memory tables
  /// </summary>
  event EventHandler<IPlaneDB<TKey, TValue>>? OnFlushMemoryTable;

  /// <summary>
  ///   Raised when merging on-disk tables
  /// </summary>
  event EventHandler<IPlaneDB<TKey, TValue>>? OnMergedTables;

  /// <summary>
  ///   Registers a merge participant
  /// </summary>
  /// <param name="participant">Participant to register</param>
  public void RegisterMergeParticipant(
    IPlaneDBMergeParticipant<TKey, TValue> participant);

  /// <summary>
  ///   Removes a registered merge participant again
  /// </summary>
  /// <param name="participant">Participant for which to remove registration</param>
  public void UnregisterMergeParticipant(
    IPlaneDBMergeParticipant<TKey, TValue> participant);
}

using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   A merge (and compaction) participant.
/// </summary>
/// <remarks>
///   Participants will only be used during merges and compaction. Implementing and registering such a participant will not
///   free users from the need to check results for staleness when using the general database APIs.
/// </remarks>
/// <typeparam name="TKey">Key type</typeparam>
/// <typeparam name="TValue">Value type</typeparam>
[PublicAPI]
public interface
  IPlaneDBMergeParticipant<TKey, TValue> : IEquatable<
  IPlaneDBMergeParticipant<TKey, TValue>>
{
  /// <summary>
  ///   Decide whether a key is stale. When stale, the key-value pairs will be removed from merged results.
  /// </summary>
  /// <remarks>
  ///   <para>Implementation must be thread safe. May be executed on any thread and concurrently and in parallel.</para>
  ///   <para>It's a bad idea to access the database from the method implementation.</para>
  /// </remarks>
  /// <param name="key">Key</param>
  /// <param name="value">Value</param>
  /// <returns>Staleness</returns>
  bool IsDataStale(in TKey key, in TValue value);
}

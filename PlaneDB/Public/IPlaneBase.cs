using System;
using System.IO;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Basic common Plane properties and functions.
/// </summary>
[PublicAPI]
public interface IPlaneBase : IDisposable
{
  /// <summary>
  ///   Base database underlying the current instance
  /// </summary>
  IPlaneDB<byte[], byte[]> BaseDB { get; }

  /// <summary>
  ///   The current number of bloom bits used throughout the DB
  /// </summary>
  long CurrentBloomBits { get; }

  /// <summary>
  ///   The current size on disk, in bytes
  /// </summary>
  long CurrentDiskSize { get; }

  /// <summary>
  ///   The current number of on disk index blocks
  /// </summary>
  long CurrentIndexBlockCount { get; }

  /// <summary>
  ///   The current size on disk without transformations, in bytes
  /// </summary>
  long CurrentRealSize { get; }

  /// <summary>
  ///   The current number of on-disk tables in use
  /// </summary>
  int CurrentTableCount { get; }

  /// <summary>
  ///   The directory where the data files are stored
  /// </summary>
  DirectoryInfo Location { get; }

  /// <summary>
  ///   Options for this instance
  /// </summary>
  PlaneOptions Options { get; }

  /// <summary>
  ///   The tablespace in use
  /// </summary>
  string TableSpace { get; }

  /// <summary>
  ///   Force a compaction.
  /// </summary>
  /// <remarks>
  ///   <para>The database will be write-locked during this operation.</para>
  ///   <para>
  ///     When compacting fully, many modifications after this point may cause more disk use until another full
  ///     compaction.
  ///   </para>
  /// </remarks>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  void Compact(CompactionMode mode = CompactionMode.Normal);

  /// <summary>
  ///   Explicitly flushes contents to disk. Content is periodically flushed implicitly and flushed one last time upon
  ///   disposing the object, so you probably do not need to call this method ever.
  /// </summary>
  [CollectionAccess(
    CollectionAccessType.Read | CollectionAccessType.ModifyExistingContent)]
  void Flush();

  /// <summary>
  ///   Insert/Update/Remove many keys in a batch.
  /// </summary>
  /// <remarks>
  ///   <para>Using this method can improve performance when modifying many keys.</para>
  ///   <para>However, please note that for the duration of the action the db/set will be locked to other readers/writers.</para>
  /// </remarks>
  /// <param name="action">Action to perform</param>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  void MassInsert([InstantHandle] Action action);

  /// <summary>
  ///   Insert/Update/Remove many keys in a batch.
  /// </summary>
  /// <remarks>
  ///   <para>Using this method can improve performance when modifying many keys.</para>
  ///   <para>However, please note that for the duration of the action the db/set will be locked to other readers/writers.</para>
  /// </remarks>
  /// <param name="action">Action to perform</param>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  TResult MassInsert<TResult>([InstantHandle] Func<TResult> action);

  /// <summary>
  ///   Read many keys in a batch.
  /// </summary>
  /// <remarks>
  ///   <para>Using this method can improve performance when reading many keys. It also acts as a Transaction</para>
  ///   <para>However, please note that for the duration of the action the db/set will be locked with a read lock.</para>
  /// </remarks>
  /// <param name="action">Action to perform</param>
  [CollectionAccess(CollectionAccessType.Read)]
  void MassRead([InstantHandle] Action action);

  /// <summary>
  ///   Read many keys in a batch.
  /// </summary>
  /// <remarks>
  ///   <para>Using this method can improve performance when reading many keys. It also acts as a Transaction</para>
  ///   <para>However, please note that for the duration of the action the db/set will be locked with a read lock.</para>
  /// </remarks>
  /// <param name="action"></param>
  /// <typeparam name="TResult">Action to perform</typeparam>
  /// <returns></returns>
  [CollectionAccess(CollectionAccessType.Read)]
  TResult MassRead<TResult>([InstantHandle] Func<TResult> action);
}

using System;
using System.IO;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   Basic common Plane properties and functions.
  /// </summary>
  [PublicAPI]
  public interface IPlaneBase : IDisposable
  {
    /// <summary>
    ///   The directory where the data files are stored
    /// </summary>
    DirectoryInfo Location { get; }

    /// <summary>
    ///   The tablespace in use
    /// </summary>
    string TableSpace { get; }

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
    ///   The current number of bloom bits used throughout the DB
    /// </summary>
    long CurrentBloomBits { get; }

    /// <summary>
    ///   Force a compaction.
    /// </summary>
    void Compact();

    /// <summary>
    ///   Explicitly flushes contents to disk. Content is periodically flushed implicitly and flushed one last time upon
    ///   disposing the object, so you probably do not need to call this method ever.
    /// </summary>
    void Flush();

    /// <summary>
    ///   Insert/Update/Remove many keys in a batch.
    /// </summary>
    /// <remarks>
    ///   <para>Using this method can improve performance when modifying many keys.</para>
    ///   <para>However, please note that for the duration of the action the db/set will be locked to other readers/writers.</para>
    /// </remarks>
    /// <param name="action"></param>
    void MassInsert(Action action);
  }
}
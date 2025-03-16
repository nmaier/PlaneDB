using System;
using System.Text;
using System.Text.RegularExpressions;

using JetBrains.Annotations;

using NMaier.BlockStream.Transformers;

namespace NMaier.PlaneDB;

/// <summary>
///   Basic Configuration
/// </summary>
/// <remarks>
///   Create a new instance of database options
/// </remarks>
/// <param name="mode">Open mode</param>
[PublicAPI]
#if NET8_0_OR_GREATER
public sealed partial class PlaneOptions(PlaneOpenMode mode = PlaneOpenMode.ReadWrite)
#else
public sealed class PlaneOptions(PlaneOpenMode mode = PlaneOpenMode.ReadWrite)
#endif
{
  /// <summary>
  ///   Allow to proceed even when the journal is broken.
  ///   If allowed, the journal will be skipped and an empty journal will be recreated, and data in it will be lost.
  /// </summary>
  public bool AllowSkippingOfBrokenJournal { get; private set; }

  /// <summary>
  ///   Allowed number of block cache entries at a time
  /// </summary>
  public int BlockCacheCapacity { get; private set; } = (int)Math.Ceiling(
    (32 << 20) / (double)BlockStream.BlockStream.BLOCK_SIZE);

  /// <summary>
  ///   The block transformer
  /// </summary>
  public IBlockTransformer BlockTransformer { get; private set; } =
    new ChecksumTransformer();

  /// <summary>
  ///   The byte-array-comparer used to internally order the db/set
  /// </summary>
  public IPlaneByteArrayComparer Comparer { get; private set; } =
    PlaneByteArrayComparer.Default;

  /// <summary>
  ///   Enable/disable the journal
  /// </summary>
  public bool JournalEnabled { get; private set; } = true;

  /// <summary>
  ///   Key cache mode to use
  /// </summary>
  public PlaneKeyCacheMode KeyCacheMode { get; private set; } =
    PlaneKeyCacheMode.NoKeyCaching;

  /// <summary>
  ///   Level0 Target Size in bytes
  /// </summary>
  public long Level0TargetSize { get; private set; } = Constants.LEVEL10_TARGET_SIZE;

  /// <summary>
  ///   Maximum number of journal operations before the journal is flushed to disk
  /// </summary>
  public int MaxJournalActions { get; private set; } = 1_000;

  /// <summary>
  ///   Open database in this mode
  /// </summary>
  public PlaneOpenMode OpenMode { get; private set; } = mode;

  /// <summary>
  ///   Databases opened with in this mode will be read-only.
  /// </summary>
  public bool ReadOnly =>
    OpenMode is PlaneOpenMode.ReadOnly or PlaneOpenMode.Packed;

  /// <summary>
  ///   ReadWrite lock to use.
  ///   This property is specified the lock will be shared between db instances using the same options or clones.
  ///   Not setting this property will create an individual default lock per db instance.
  /// </summary>
  public IPlaneReadWriteLock? ReadWriteLock { get; private set; }

  /// <summary>
  ///   Called whenever a repair action was taken in repair mode.
  /// </summary>
  public Action<IPlaneBase, PlaneRepairEventArgs>? RepairCallback { get; private set; }

  /// <summary>
  ///   Run in repair mode, trying to autocorrect things
  /// </summary>
  public bool RepairMode { get; private set; }

  /// <summary>
  ///   The tablespace to use
  /// </summary>
  public string Tablespace { get; set; } = "default";

  /// <summary>
  ///   Configured thread-safety
  /// </summary>
  public bool ThreadSafe { get; private set; } = true;

  internal IPlaneReadWriteLock TrueReadWriteLock =>
    ReadWriteLock ?? new ReadWriteLock();

  /// <summary>
  ///   Activates repair mode, trying to salvage as much data as possible
  /// </summary>
  /// <returns>New options with repair mode activated</returns>
  public PlaneOptions ActivateRepairMode(
    Action<IPlaneBase, PlaneRepairEventArgs>? callback = null)
  {
    var rv = Clone();
    rv.OpenMode = PlaneOpenMode.Repair;
    rv.RepairMode = true;
    rv.RepairCallback = callback;
    rv.AllowSkippingOfBrokenJournal = true;

    return rv;
  }

  /// <summary>
  ///   Clone this instance of options
  /// </summary>
  /// <returns>Clone of this options</returns>
  public PlaneOptions Clone()
  {
    return (PlaneOptions)MemberwiseClone();
  }

  /// <summary>
  ///   Disable the journal
  /// </summary>
  /// <remarks>Disabling the journal may cause data-loss if the db/set is not properly disposed (e.g. on crashes)</remarks>
  /// <returns>New options with journal disabled</returns>
  /// <seealso cref="JournalEnabled" />
  public PlaneOptions DisableJournal()
  {
    var rv = Clone();
    rv.JournalEnabled = false;

    return rv;
  }

  /// <summary>
  ///   Disables thread-safety, which can improve performance for certain single-threaded applications
  /// </summary>
  /// <remarks>
  ///   Do not disable thread-safety when you explicitly or implicitly access the db/set from multiple threads. This
  ///   includes Tasks/async-await programming. Disabling thread-safety when using threads will cause data corruption!
  /// </remarks>
  /// <returns>New options with thread-safety disabled</returns>
  /// <seealso cref="ThreadSafe" />
  public PlaneOptions DisableThreadSafety()
  {
    var rv = Clone();
    rv.ThreadSafe = false;

    return rv;
  }

  /// <summary>
  ///   Configure journal to get flushed after at least this number of writes
  /// </summary>
  /// <param name="writes">Number of writes after which to force a journal flush</param>
  /// <remarks>
  ///   This option lets you trade data security vs performance. If writes are batched (write > 1), then journal disk
  ///   activity is reduced, but hard application/system crashes may lose the latest few writes
  /// </remarks>
  /// <returns>New options with journal write flushing configured</returns>
  /// <seealso cref="MaxJournalActions" />
  public PlaneOptions FlushJournalAfterNumberOfWrites(int writes)
  {
#if NET8_0_OR_GREATER
    ArgumentOutOfRangeException.ThrowIfNegative(writes, nameof(writes));
#else
    if (writes < 0) {
      throw new ArgumentOutOfRangeException(nameof(writes));
    }
#endif

    var rv = Clone();
    rv.MaxJournalActions = writes;

    return rv;
  }

  /// <summary>
  ///   Gets the transformer to actually use, either <see cref="BlockTransformer" /> or the result of
  ///   <see cref="IPlaneSaltableBlockTransformer.GetTransformerFor" />
  /// </summary>
  /// <param name="salt">Salt to use when configured transformer supports salting</param>
  /// <returns>Final block transformer</returns>
  public IBlockTransformer GetTransformerFor(ReadOnlySpan<byte> salt)
  {
    return BlockTransformer is IPlaneSaltableBlockTransformer sbt
      ? sbt.GetTransformerFor(salt)
      : BlockTransformer;
  }

  /// <summary>
  ///   Makes journal writes fully synchronous, meaning every write is flushed immediately.
  /// </summary>
  /// <remarks>Best for data security, but will impact performance</remarks>
  /// <seealso cref="FlushJournalAfterNumberOfWrites" />
  /// <returns>New options with journal configured to be fully sync</returns>
  /// <seealso cref="MaxJournalActions" />
  public PlaneOptions MakeFullySync()
  {
    var rv = Clone();
    rv.MaxJournalActions = -1;

    return rv;
  }

  /// <summary>
  ///   Makes journal writes fully synchronous, meaning every write is flushed immediately to the OS but not necessarily the
  ///   disk.
  /// </summary>
  /// <remarks>Best for data security, but will impact performance</remarks>
  /// <seealso cref="FlushJournalAfterNumberOfWrites" />
  /// <returns>New options with journal configured to be fully sync</returns>
  /// <seealso cref="MaxJournalActions" />
  public PlaneOptions MakeMostlySync()
  {
    var rv = Clone();
    rv.MaxJournalActions = 0;

    return rv;
  }

  /// <summary>
  ///   Skip broken journals.
  /// </summary>
  /// <returns>New options with skipping of broken journals enabled</returns>
  /// <seealso cref="AllowSkippingOfBrokenJournal" />
  public PlaneOptions SkipBrokenJournal()
  {
    var rv = Clone();
    rv.AllowSkippingOfBrokenJournal = true;

    return rv;
  }

  /// <summary>
  ///   Use a certain table space
  /// </summary>
  /// <param name="space">The space to use</param>
  /// <remarks>
  ///   If not configured, the default tablespace "default" will be used. This option will be ignored when opened in
  ///   <see cref="PlaneOpenMode.Packed" /> mode
  /// </remarks>
  /// <returns>New options with tablespace configured</returns>
  /// <seealso cref="Tablespace" />
  public PlaneOptions UsingTablespace(string space)
  {
    if (string.IsNullOrEmpty(space)) {
      throw new ArgumentException("Tablespace cannot be empty", nameof(space));
    }

    if (!spaceValidator.IsMatch(space)) {
      throw new ArgumentException("Tablespace has invalid characters", nameof(space));
    }

    var rv = Clone();
    rv.Tablespace = space;

    return rv;
  }

  /// <summary>
  ///   Configure the block cache size, meaning how many disk blocks are allowed to be cached in memory at a time.
  /// </summary>
  /// <param name="sizeInBytes">Size of the cache in bytes (approximate)</param>
  /// <returns>New options with block cache size configured</returns>
  /// <seealso cref="BlockCacheCapacity" />
  public PlaneOptions WithBlockCacheByteSize(long sizeInBytes)
  {
#if NET8_0_OR_GREATER
    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sizeInBytes, nameof(sizeInBytes));
#else
    if (sizeInBytes <= 0) {
      throw new ArgumentOutOfRangeException(nameof(sizeInBytes));
    }
#endif

    var rv = Clone();
    rv.BlockCacheCapacity =
      (int)Math.Ceiling(sizeInBytes / (double)BlockStream.BlockStream.BLOCK_SIZE);

    return rv.BlockCacheCapacity > 0
      ? rv
      : throw new ArgumentOutOfRangeException(nameof(sizeInBytes));
  }

  /// <summary>
  ///   Configure the block cache size, meaning how many disk blocks are allowed to be cached in memory at a time. A block is
  ///   usually several kilobytes in size; keep this in mind when configuring this option.
  /// </summary>
  /// <param name="capacity">Size of the cache</param>
  /// <returns>New options with block cache size configured</returns>
  /// <seealso cref="BlockCacheCapacity" />
  public PlaneOptions WithBlockCacheCapacity(int capacity)
  {
    if (capacity is < 1 or > 100_000) {
      throw new ArgumentOutOfRangeException(nameof(capacity));
    }

    var rv = Clone();
    rv.BlockCacheCapacity = capacity;

    return rv;
  }

  /// <summary>
  ///   Explicitly set a custom block transformer
  /// </summary>
  /// <param name="transformer">The block transformer to use</param>
  /// <returns>New options with block transformer configured</returns>
  /// <seealso cref="BlockTransformer" />
  public PlaneOptions WithBlockTransformer(IBlockTransformer transformer)
  {
    var rv = Clone();
    rv.BlockTransformer =
      transformer ?? throw new ArgumentNullException(nameof(transformer));

    return rv;
  }

  /// <summary>
  ///   Configure the byte comparer implementation
  /// </summary>
  /// <param name="comparer">Comparer to use</param>
  /// <returns>New options with comparer configured</returns>
  /// <seealso cref="Comparer" />
  /// <remarks>The block comparer must be deterministic and SHOULD sort byte arrays lexically</remarks>
  public PlaneOptions WithByteComparer(IPlaneByteArrayComparer comparer)
  {
    var rv = Clone();
    rv.Comparer = comparer;

    return rv;
  }

  /// <summary>
  ///   Enables compression (thus disabling encryption, if any)
  /// </summary>
  /// <returns>New options with the block transformer set to the <see cref="LZ4CompressorTransformer" /></returns>
  /// <seealso cref="BlockTransformer" />
  public PlaneOptions WithCompression()
  {
    var rv = Clone();
    rv.BlockTransformer = new LZ4CompressorTransformer();

    return rv;
  }

  /// <summary>
  ///   Use a default lock.
  ///   Any options objects originating from this options object will use the same lock!
  ///   It is generally not necessary to call this method, unless you want to create such a shared lock.
  /// </summary>
  /// <seealso cref="ReadWriteLock" />
  /// <returns>New options with the lock set to a default lock instance</returns>
  public PlaneOptions WithDefaultLock()
  {
    var rv = Clone();
    rv.ReadWriteLock = new ReadWriteLock();

    return rv;
  }

  /// <summary>
  ///   Enables encryption (and compression)
  /// </summary>
  /// <param name="passphrase">Encryption pass-phrase</param>
  /// <returns>New options with the block transformer set to the <see cref="EncryptedCompressedTransformer" /></returns>
  /// <seealso cref="BlockTransformer" />
  public PlaneOptions WithEncryption(string passphrase)
  {
    return WithEncryption(Encoding.UTF8.GetBytes(passphrase));
  }

  /// <summary>
  ///   Enables encryption (and compression)
  /// </summary>
  /// <param name="passphrase">Encryption pass-phrase</param>
  /// <returns>New options with the block transformer set to the <see cref="EncryptedCompressedTransformer" /></returns>
  /// <seealso cref="BlockTransformer" />
  public PlaneOptions WithEncryption(byte[] passphrase)
  {
    var rv = Clone();
    rv.BlockTransformer = new EncryptionWithSaltTransformer(passphrase);

    return rv;
  }

  /// <summary>
  ///   Use a specific key cache mode.
  /// </summary>
  /// <param name="mode">Key cache mode to use</param>
  /// <returns>New options with the key cache mode set</returns>
  public PlaneOptions WithKeyCacheMode(PlaneKeyCacheMode mode)
  {
    var rv = Clone();
    rv.KeyCacheMode = mode switch {
      PlaneKeyCacheMode.AutoKeyCaching => mode,
      PlaneKeyCacheMode.NoKeyCaching => mode,
      PlaneKeyCacheMode.ForceKeyCaching => mode,
      _ => throw new ArgumentOutOfRangeException(nameof(mode))
    };

    return rv;
  }

  /// <summary>
  ///   Use a specific target0 size.
  /// </summary>
  /// <param name="level0TargetSize">Size to use</param>
  /// <returns>New options with the level0 size set</returns>
  public PlaneOptions WithLevel0TargetSize(PlaneLevel0TargetSize level0TargetSize)
  {
    var ts = (long)level0TargetSize;
    if (ts < (long)PlaneLevel0TargetSize.DefaultSize ||
        ts % (long)PlaneLevel0TargetSize.DefaultSize != 0) {
      throw new ArgumentOutOfRangeException(nameof(level0TargetSize));
    }

    var rv = Clone();
    rv.Level0TargetSize = ts;

    return rv;
  }

  /// <summary>
  ///   Use this lock instance.
  ///   Any options objects originating from this options object will use this same lock!
  ///   It is generally not necessary to call this method, unless you want to create such a shared lock.
  /// </summary>
  /// <seealso cref="ReadWriteLock" />
  /// <returns>New options with the lock set to this lock instance</returns>
  public PlaneOptions WithLock(IPlaneReadWriteLock readWriteLock)
  {
    var rv = Clone();
    rv.ReadWriteLock = readWriteLock ??
                       throw new ArgumentNullException(nameof(readWriteLock));

    return rv;
  }

  /// <summary>
  ///   Switch the open mode
  /// </summary>
  /// <param name="mode">Open mode</param>
  /// <returns></returns>
  public PlaneOptions WithOpenMode(PlaneOpenMode mode)
  {
    switch (mode) {
      case PlaneOpenMode.Packed:
      case PlaneOpenMode.ReadOnly:
      case PlaneOpenMode.CreateReadWrite:
      case PlaneOpenMode.ExistingReadWrite:
      case PlaneOpenMode.ReadWrite:
        break;
      case PlaneOpenMode.Repair:
        throw new InvalidOperationException("Use ActivateRepairMode to use repair mode!");
      default:
        throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
    }

    var rv = Clone();
    rv.OpenMode = mode;

    return rv;
  }

#if NET8_0_OR_GREATER
  [GeneratedRegex(
    "^[a-z0-9_.-]+$",
    RegexOptions.IgnoreCase | RegexOptions.Compiled,
    "en-US")]
  private static partial Regex SpaceValidator();

  private static readonly Regex spaceValidator = SpaceValidator();
#else
  private static readonly Regex spaceValidator = new(
    "^[a-z0-9_.-]+$",
    RegexOptions.IgnoreCase | RegexOptions.Compiled);
#endif
}

using System;
using System.Text;
using System.Text.RegularExpressions;
using JetBrains.Annotations;
using NMaier.BlockStream;

namespace NMaier.PlaneDB
{
  /// <summary>
  ///   Basic Configuration
  /// </summary>
  [PublicAPI]
  public sealed class PlaneDBOptions
  {
    /// <summary>
    ///   Allowed number of block cache entries at a time
    /// </summary>
    public int BlockCacheCapacity { get; private set; } = (int)Math.Ceiling((32 << 20) / (double)BlockStream.BlockStream.BLOCK_SIZE);

    /// <summary>
    ///   The block transformer
    /// </summary>
    public IBlockTransformer BlockTransformer { get; private set; } = new ChecksumTransformer();

    /// <summary>
    ///   The byte-array-comparer used to interally order the db/set
    /// </summary>
    public IByteArrayComparer Comparer { get; private set; } = new ByteArrayComparer();

    /// <summary>
    ///   Enable/disable the journal
    /// </summary>
    public bool JournalEnabled { get; private set; } = true;

    /// <summary>
    ///   Maximum number of journal operations before the journal is flushed to disk
    /// </summary>
    public int MaxJournalActions { get; private set; } = 1_000;

    /// <summary>
    ///   The tablespace to use
    /// </summary>
    public string TableSpace { get; set; } = string.Empty;

    /// <summary>
    ///   Configured thread-safety
    /// </summary>
    public bool ThreadSafe { get; private set; } = true;

    /// <summary>
    ///   Clone this instance of options
    /// </summary>
    /// <returns></returns>
    public PlaneDBOptions Clone()
    {
      return (PlaneDBOptions)MemberwiseClone();
    }

    /// <summary>
    ///   Disable the journal
    /// </summary>
    /// <remarks>Disabling the journal may cause dataloss if the db/set is not properly disposed (e.g. on crashes)</remarks>
    /// <returns>New options with journal disabled</returns>
    /// <seealso cref="JournalEnabled" />
    public PlaneDBOptions DisableJournal()
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
    public PlaneDBOptions DisableThreadSafety()
    {
      var rv = Clone();
      rv.ThreadSafe = false;
      return rv;
    }

    /// <summary>
    ///   Enables compression (thus disabling encryption, if any)
    /// </summary>
    /// <returns>New options with the block transformer set to the <see cref="LZ4CompressorTransformer" /></returns>
    /// <seealso cref="BlockTransformer" />
    public PlaneDBOptions EnableCompression()
    {
      var rv = Clone();
      rv.BlockTransformer = new LZ4CompressorTransformer();
      return rv;
    }

    /// <summary>
    ///   Enables encryption (and compression)
    /// </summary>
    /// <param name="passphrase">Encryption passphrase</param>
    /// <returns>New options with the block transformer set to the <see cref="EncryptedCompressedTransformer" /></returns>
    /// <seealso cref="BlockTransformer" />
    public PlaneDBOptions EnableEncryption(string passphrase)
    {
      return EnableEncryption(Encoding.UTF8.GetBytes(passphrase));
    }

    /// <summary>
    ///   Enables encryption (and compression)
    /// </summary>
    /// <param name="passphrase">Encryption passphrase</param>
    /// <returns>New options with the block transformer set to the <see cref="EncryptedCompressedTransformer" /></returns>
    /// <seealso cref="BlockTransformer" />
    public PlaneDBOptions EnableEncryption(byte[] passphrase)
    {
      var rv = Clone();
      rv.BlockTransformer = new EncryptedCompressedTransformer(passphrase);
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
    public PlaneDBOptions FlushJournalAfterNumberOfWrites(int writes)
    {
      if (writes < 0) {
        throw new ArgumentOutOfRangeException(nameof(writes));
      }

      var rv = Clone();
      rv.MaxJournalActions = writes;
      return rv;
    }

    /// <summary>
    ///   Makes journal writes fully synchronous, meaning every write is flushed immediately.
    /// </summary>
    /// <remarks>Best for data security, but will impact performance</remarks>
    /// <seealso cref="FlushJournalAfterNumberOfWrites" />
    /// <returns>New options with journal configured to be fully sync</returns>
    /// <seealso cref="MaxJournalActions" />
    public PlaneDBOptions MakeFullySync()
    {
      var rv = Clone();
      rv.MaxJournalActions = 0;
      return rv;
    }

    /// <summary>
    ///   Explicitly set a custom block transformer
    /// </summary>
    /// <param name="transformer">The block tranformer to use</param>
    /// <returns>New options with blocktransformer configured</returns>
    /// <seealso cref="BlockTransformer" />
    public PlaneDBOptions UsingBlockTransformer(IBlockTransformer transformer)
    {
      var rv = Clone();
      rv.BlockTransformer = transformer ?? throw new ArgumentNullException(nameof(transformer));
      return rv;
    }

    /// <summary>
    ///   Use a certain table space
    /// </summary>
    /// <param name="space">The space to use</param>
    /// <remarks>If not configured, the default tablespace "default" will be used</remarks>
    /// <returns>New options with tablespace configured</returns>
    /// <seealso cref="TableSpace" />
    public PlaneDBOptions UsingTableSpace(string space)
    {
      var rv = Clone();
      rv.TableSpace = space;
      return rv;
    }

    /// <summary>
    ///   Configure the block cache size, meaning how many disk blocks are allowed to be cached in memory at a time. A block is
    ///   usually several kilobytes in size; keep this in mind when configuring this option.
    /// </summary>
    /// <param name="capacity">Size of the cache</param>
    /// <returns>New options with block cache size configured</returns>
    /// <seealso cref="BlockCacheCapacity" />
    public PlaneDBOptions WithBlockCacheCapacity(int capacity)
    {
      if (capacity < 0) {
        throw new ArgumentOutOfRangeException(nameof(capacity));
      }

      var rv = Clone();
      rv.BlockCacheCapacity = capacity;
      return rv;
    }


    /// <summary>
    ///   Configure the block cache size, meaning how many disk blocks are allowed to be cached in memory at a time.
    /// </summary>
    /// <param name="sizeInBytes">Size of the cache in bytes (approximate)</param>
    /// <returns>New options with block cache size configured</returns>
    /// <seealso cref="BlockCacheCapacity" />
    public PlaneDBOptions WithBlockCacheByteSize(long sizeInBytes)
    {
      if (sizeInBytes < 0) {
        throw new ArgumentOutOfRangeException(nameof(sizeInBytes));
      }

      var rv = Clone();
      rv.BlockCacheCapacity = (int)Math.Ceiling(sizeInBytes / (double)BlockStream.BlockStream.BLOCK_SIZE);
      return rv;
    }


    /// <summary>
    ///   Configure the byte comparer implementation
    /// </summary>
    /// <param name="comparer">Comparer to use</param>
    /// <returns>New options with comparer configured</returns>
    /// <seealso cref="Comparer" />
    /// <remarks>The block comparer must be deterministic and SHOULD sort byte arrays lexically</remarks>
    public PlaneDBOptions WithComparer(IByteArrayComparer comparer)
    {
      var rv = Clone();
      rv.Comparer = comparer;
      return rv;
    }

    internal void Validate()
    {
      if (!Regex.IsMatch(TableSpace, @"^[a-z0-9_-]*$", RegexOptions.IgnoreCase)) {
        throw new ArgumentException("Invalid table space", nameof(TableSpace));
      }

      if (BlockCacheCapacity < 1 || BlockCacheCapacity > 100_000) {
        throw new ArgumentOutOfRangeException(nameof(BlockCacheCapacity));
      }

      if (MaxJournalActions < 0) {
        throw new ArgumentOutOfRangeException(nameof(MaxJournalActions));
      }
    }
  }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   Your byte[]-to-byte[] persistent key-value store!
/// </summary>
/// <remarks>
///   <list type="bullet">
///     <item>
///       <description>Thread-safe unless configured otherwise.</description>
///     </item>
///     <item>
///       <description>All write (add/update/remove) operations may raise I/O exceptions.</description>
///     </item>
///   </list>
/// </remarks>
[PublicAPI]
public sealed partial class PlaneDB : IPlaneDB<byte[], byte[]>
{
  [MethodImpl(MethodImplOptions.NoInlining)]
  private static void ThrowKeyExists()
  {
    throw new ArgumentException("Key exists");
  }

  [MethodImpl(MethodImplOptions.NoInlining)]
  private static void ThrowKeyNotFoundException()
  {
    throw new KeyNotFoundException();
  }

  [MethodImpl(MethodImplOptions.NoInlining)]
  private static void ThrowReadOnly()
  {
    throw new PlaneDBReadOnlyException();
  }

  private readonly BackgroundActionQueue backgroundQueue = new();
  private readonly BlockCache blockCache;
  private readonly byte[] family = [];
  private readonly long largeValueSize;
  private readonly long level0TargetSize;
  private readonly IPlaneDBState state;
  private readonly SortedDictionary<byte, long> targetLevelSizes;
  private bool allowMerge = true;
  private int disposed;
  private IMemoryTable memoryTable;
  private KeyValuePair<ulong, ISSTable>[] tables = [];

  /// <summary>
  ///   Opens or creates a new PlaneDB.
  /// </summary>
  /// <param name="location">Directory that will store the PlaneDB</param>
  /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  public PlaneDB(DirectoryInfo location, PlaneOptions options)
  {
    Location = location;

    Options = options.Clone();
    blockCache = new BlockCache(options.BlockCacheCapacity);
    memoryTable = !options.ReadOnly
      ? new MemoryTable(options, 0)
      : new MemoryTableReadOnly();
    level0TargetSize = options.Level0TargetSize;
    largeValueSize = (long)(level0TargetSize * 0.85);
    var targetSizes = Enumerable.Range(0, MAX_EXTENDED_LEVEL + 1)
      .Select(i => new KeyValuePair<byte, long>((byte)i, level0TargetSize * (1L << i)))
      .ToDictionary(i => i.Key, i => i.Value);
    targetLevelSizes = new SortedDictionary<byte, long>(targetSizes);

    state = options.OpenMode switch {
      PlaneOpenMode.Packed => new PlaneDBStatePacked(new FileInfo(location.FullName)),
      PlaneOpenMode.ReadOnly => new PlaneDBStateReadOnly(location, options),
      _ => new PlaneDBState(location, options)
    };

    try {
      ReopenSSTables();
    }
    catch {
      state.Dispose();

      throw;
    }

    MaybeMergeSmallTail();

    if (!options.ThreadSafe || options.ReadOnly) {
      return;
    }

    mergeThread = new Thread(MergeLoop) {
      Priority = ThreadPriority.BelowNormal,
      Name = $"Plane-Background-Merge-{Location.Name}-{TableSpace}"
    };
    mergeThread.Start();
  }

  /// <summary>
  ///   All levels and corresponding identifiers in this DB
  /// </summary>
  public SortedList<byte, ulong[]> AllLevels => state.GetAllLevels(family);

  internal byte[] Salt => state.Salt;

  /// <summary>
  ///   All identifiers in the sequence they are considered
  /// </summary>
  public ulong[] TableSequence => state.Sequence(family).ToArray();

  /// <inheritdoc />
  public async IAsyncEnumerator<KeyValuePair<byte[], byte[]>> GetAsyncEnumerator(
    CancellationToken cancellationToken = new())
  {
    await Task.Yield();
    using var e = GetEnumerator();
    while (e.MoveNext()) {
      cancellationToken.ThrowIfCancellationRequested();

      yield return e.Current;
    }
  }

  /// <inheritdoc />
  public void Add(KeyValuePair<byte[], byte[]> item)
  {
    if (!TryAdd(item.Key, item.Value)) {
      ThrowKeyExists();
    }
  }

  /// <inheritdoc />
  public void Clear()
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    using (state.ReadWriteLock.AcquireWriteLock()) {
      memoryTable = new MemoryTable(Options, unchecked(memoryTable.Generation + 1));
      state.ClearJournal();
      state.ClearManifest();
      ReopenSSTables();
    }
  }

  /// <inheritdoc />
  public bool Contains(KeyValuePair<byte[], byte[]> item)
  {
    return ContainsKey(item.Key);
  }

  /// <inheritdoc />
  public void CopyTo(KeyValuePair<byte[], byte[]>[] array, int arrayIndex)
  {
    foreach (var keyValuePair in this) {
      array[arrayIndex++] = keyValuePair;
    }
  }

  /// <inheritdoc />
  public int Count => GetInternalEnumerable(false).Count();

  /// <inheritdoc />
  public bool IsReadOnly => Options.ReadOnly;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(KeyValuePair<byte[], byte[]> item)
  {
    return Remove(item.Key);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Add(byte[] key, byte[] value)
  {
    if (!TryAdd(key, value)) {
      ThrowKeyExists();
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool ContainsKey(byte[] key)
  {
    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireReadLock()) {
      return ContainsKeyUnlocked(key, hashes);
    }
  }

  /// <inheritdoc />
  public byte[] this[byte[] key]
  {
    [CollectionAccess(CollectionAccessType.Read)]
    get
    {
      if (!TryGetValue(key, out var val)) {
        ThrowKeyNotFoundException();
      }

      return val!;
    }
    [CollectionAccess(CollectionAccessType.UpdatedContent)]
    set => SetValue(key, value);
  }

  /// <inheritdoc />
  public ICollection<byte[]> Keys =>
    GetInternalEnumerable(false).Select(i => i.Key).ToArray();

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(byte[] key)
  {
    return TryRemove(key, out _);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryGetValue(byte[] key, [MaybeNullWhen(false)] out byte[] value)
  {
    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireReadLock()) {
      return TryGetValueUnlocked(key, hashes, out value);
    }
  }

  /// <inheritdoc />
  public ICollection<byte[]> Values =>
    GetInternalEnumerable(true).Select(i => i.Value).ToArray();

  /// <inheritdoc />
  public void Dispose()
  {
#if NET8_0_OR_GREATER
    ObjectDisposedException.ThrowIf(
      Interlocked.CompareExchange(ref disposed, 1, 0) != 0,
      typeof(PlaneDB));
#else
    if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) {
      throw new ObjectDisposedException(nameof(PlaneDB));
    }
#endif

    allowMerge = false;
    Flush();
    mergeRequests.CompleteAdding();
    mergeThread?.Join();

    backgroundQueue.Dispose();

    foreach (var t in tables) {
      t.Value.Dispose();
    }

    state.Dispose();
    blockCache.Dispose();
  }

  IEnumerator IEnumerable.GetEnumerator()
  {
    return GetEnumerator();
  }

  /// <inheritdoc />
  public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
  {
    return GetInternalEnumerator(true);
  }

  /// <inheritdoc />
  public IPlaneDB<byte[], byte[]> BaseDB => this;

  /// <inheritdoc />
  public void Compact(CompactionMode mode = CompactionMode.Normal)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    using (state.ReadWriteLock.AcquireWriteLock()) {
      allowMerge = false;
      try {
        FlushUnlocked();
      }
      finally {
        allowMerge = true;
      }

      if (mode == CompactionMode.Normal) {
        CompactLevels();
      }
      else {
        BuildSuper();
      }

      state.MaybeCompactManifest();
    }
  }

  /// <inheritdoc />
  public long CurrentBloomBits => tables.Sum(t => t.Value.BloomBits);

  /// <inheritdoc />
  public long CurrentDiskSize => tables.Sum(t => t.Value.DiskSize);

  /// <inheritdoc />
  public long CurrentIndexBlockCount => tables.Sum(t => t.Value.IndexBlockCount);

  /// <inheritdoc />
  public long CurrentRealSize => tables.Sum(t => t.Value.RealSize);

  /// <inheritdoc />
  public int CurrentTableCount { get; private set; }

  /// <inheritdoc />
  public void Flush()
  {
    if (Options.ReadOnly) {
      return;
    }

    using (state.ReadWriteLock.AcquireWriteLock()) {
      FlushUnlocked();
    }
  }

  /// <inheritdoc />
  public DirectoryInfo Location { get; }

  /// <inheritdoc />
  public void MassInsert([InstantHandle] Action action)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    using (state.ReadWriteLock.AcquireWriteLock()) {
      allowMerge = false;
      try {
        action();
      }
      finally {
        allowMerge = true;
        FlushUnlocked();
        MaybeMerge();
      }
    }
  }

  /// <inheritdoc />
  public TResult MassInsert<TResult>(Func<TResult> action)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    using (state.ReadWriteLock.AcquireWriteLock()) {
      allowMerge = false;
      try {
        return action();
      }
      finally {
        allowMerge = true;
        FlushUnlocked();
        MaybeMerge();
      }
    }
  }

  /// <inheritdoc />
  public void MassRead(Action action)
  {
    using (state.ReadWriteLock.AcquireReadLock()) {
      action();
    }
  }

  /// <inheritdoc />
  public TResult MassRead<TResult>(Func<TResult> action)
  {
    using (state.ReadWriteLock.AcquireReadLock()) {
      return action();
    }
  }

  /// <inheritdoc />
  public PlaneOptions Options { get; private set; }

  /// <inheritdoc />
  public string TableSpace => Options.Tablespace;

  /// <inheritdoc />
  public event EventHandler<IPlaneDB<byte[], byte[]>>? OnFlushMemoryTable;

  /// <inheritdoc />
  public event EventHandler<IPlaneDB<byte[], byte[]>>? OnMergedTables;

  /// <inheritdoc />
  public byte[] AddOrUpdate(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.ValueFactory addValueFactory,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactory updateValueFactory)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      byte[] newValue;
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        newValue = updateValueFactory(existingValue);
        if (newValue.AsSpan().SequenceEqual(existingValue)) {
          return newValue;
        }
      }
      else {
        newValue = addValueFactory();
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return newValue;
      }
    }
  }

  /// <inheritdoc />
  public byte[] AddOrUpdate(
    byte[] key,
    byte[] addValue,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactory updateValueFactory)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        addValue = updateValueFactory(existingValue);
        if (addValue.AsSpan().SequenceEqual(existingValue)) {
          return addValue;
        }
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, addValue);

        return addValue;
      }
    }
  }

  /// <inheritdoc />
  public byte[] AddOrUpdate<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithArg<TArg> addValueFactory,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactoryWithArg<TArg> updateValueFactory,
    TArg factoryArgument)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      byte[] newValue;
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        newValue = updateValueFactory(existingValue, factoryArgument);
        if (newValue.AsSpan().SequenceEqual(existingValue)) {
          return newValue;
        }
      }
      else {
        newValue = addValueFactory(factoryArgument);
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return newValue;
      }
    }
  }

  /// <inheritdoc />
  public void CopyTo(IDictionary<byte[], byte[]> destination)
  {
    if (destination is PlaneDB other) {
      other.MassInsert(
        () => {
          foreach (var (key, value) in this) {
            other.SetValue(key, value);
          }
        });

      return;
    }

    foreach (var (key, value) in this) {
      destination[key] = value;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetKeysIteratorAsync(
    [EnumeratorCancellation] CancellationToken token)
  {
    await foreach (var keyValuePair in
                   this.WithCancellation(token).ConfigureAwait(false)) {
      yield return keyValuePair.Key;
    }
  }

  /// <inheritdoc />
  public byte[] GetOrAdd(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.ValueFactory valueFactory)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (TryGetValueUnlocked(key, hashes, out var value)) {
        return value;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (TryGetValueFromMemoryUnlocked(key, hashes, out var value)) {
          return value;
        }
      }
      else if (TryGetValueUnlocked(key, hashes, out var value)) {
        return value;
      }

      var newValue = valueFactory();
      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return newValue;
      }
    }
  }

  /// <inheritdoc />
  public byte[] GetOrAdd(byte[] key, byte[] value)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        return existingValue;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (TryGetValueFromMemoryUnlocked(key, hashes, out var existingValue)) {
          return existingValue;
        }
      }
      else if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        return existingValue;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, value);

        return value;
      }
    }
  }

  /// <inheritdoc />
  public byte[] GetOrAdd(byte[] key, byte[] value, out bool added)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        added = false;

        return existingValue;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (TryGetValueFromMemoryUnlocked(key, hashes, out var existingValue)) {
          added = false;

          return existingValue;
        }
      }
      else if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        added = false;

        return existingValue;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, value);
        added = true;

        return value;
      }
    }
  }

  /// <inheritdoc />
  public byte[] GetOrAdd<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
        return existingValue;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (TryGetValueFromMemoryUnlocked(key, hashes, out var value)) {
          return value;
        }
      }
      else if (TryGetValueUnlocked(key, hashes, out var value)) {
        return value;
      }

      var newValue = valueFactory(factoryArgument);
      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return newValue;
      }
    }
  }

  /// <inheritdoc />
  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    [InstantHandle] IEnumerable<KeyValuePair<byte[], byte[]>> keysAndDefaults)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    foreach (var kv in keysAndDefaults) {
      var hashes = new BloomFilter.Hashes(kv.Key);
      KeyValuePair<byte[], byte[]> rv;
      using (state.ReadWriteLock.AcquireUpgradableLock()) {
        if (TryGetValueUnlocked(kv.Key, hashes, out var existingValue)) {
          rv = new KeyValuePair<byte[], byte[]>(kv.Key, existingValue);
        }
        else {
          using (state.ReadWriteLock.AcquireWriteLock()) {
            PutValueUnlocked(kv.Key, kv.Value);
          }

          rv = kv;
        }
      }

      yield return rv;
    }
  }

  /// <inheritdoc />
  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    [InstantHandle] IEnumerable<byte[]> keys,
    byte[] value)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    foreach (var key in keys) {
      var hashes = new BloomFilter.Hashes(key);
      KeyValuePair<byte[], byte[]> rv;
      using (state.ReadWriteLock.AcquireUpgradableLock()) {
        if (TryGetValueUnlocked(key, hashes, out var existingValue)) {
          rv = new KeyValuePair<byte[], byte[]>(key, existingValue);
        }
        else {
          using (state.ReadWriteLock.AcquireWriteLock()) {
            PutValueUnlocked(key, value);
          }

          rv = new KeyValuePair<byte[], byte[]>(key, value);
        }
      }

      yield return rv;
    }
  }

  /// <inheritdoc />
  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    [InstantHandle] IEnumerable<byte[]> keys,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.ValueFactoryWithKey valueFactory)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    foreach (var key in keys) {
      var hashes = new BloomFilter.Hashes(key);
      KeyValuePair<byte[], byte[]> rv;
      using (state.ReadWriteLock.AcquireUpgradableLock()) {
        if (TryGetValueUnlocked(key, hashes, out var value)) {
          rv = new KeyValuePair<byte[], byte[]>(key, value);
        }
        else {
          value = valueFactory(key);
          using (state.ReadWriteLock.AcquireWriteLock()) {
            PutValueUnlocked(key, value);
          }

          rv = new KeyValuePair<byte[], byte[]>(key, value);
        }
      }

      yield return rv;
    }
  }

  /// <inheritdoc />
  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange<TArg>(
    IEnumerable<byte[]> keys,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithKeyAndArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    foreach (var key in keys) {
      var hashes = new BloomFilter.Hashes(key);
      KeyValuePair<byte[], byte[]> rv;
      using (state.ReadWriteLock.AcquireUpgradableLock()) {
        if (TryGetValueUnlocked(key, hashes, out var value)) {
          rv = new KeyValuePair<byte[], byte[]>(key, value);
        }
        else {
          value = valueFactory(key, factoryArgument);
          using (state.ReadWriteLock.AcquireWriteLock()) {
            PutValueUnlocked(key, value);
          }

          rv = new KeyValuePair<byte[], byte[]>(key, value);
        }
      }

      yield return rv;
    }
  }

  /// <inheritdoc />
  public IEnumerable<byte[]> KeysIterator =>
    GetInternalEnumerable(false).Select(i => i.Key);

  /// <inheritdoc />
  public void SetValue(byte[] key, byte[] value)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (TryGetValueUnlocked(key, hashes, out var existing) &&
          value.AsSpan().SequenceEqual(existing)) {
        return;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, value);
      }
    }
  }

  /// <inheritdoc />
  public bool TryAdd(byte[] key, byte[] value)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (ContainsKeyUnlocked(key, hashes)) {
        return false;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (memoryTable.ContainsKey(key, hashes, out var removed) && !removed) {
          return false;
        }
      }
      else if (ContainsKeyUnlocked(key, hashes)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, value);

        return true;
      }
    }
  }

  /// <inheritdoc />
  public bool TryAdd(byte[] key, byte[] value, [MaybeNullWhen(true)] out byte[] existing)
  {
    long last;
    var hashes = new BloomFilter.Hashes(key);

    using (state.ReadWriteLock.AcquireReadLock()) {
      if (TryGetValueUnlocked(key, hashes, out existing)) {
        return false;
      }

      last = memoryTable.Generation;
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (last == memoryTable.Generation) {
        if (TryGetValueFromMemoryUnlocked(key, hashes, out existing)) {
          return false;
        }
      }
      else if (TryGetValueUnlocked(key, hashes, out existing)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, value);

        return true;
      }
    }
  }

  /// <inheritdoc />
  public (long, long) TryAdd(IEnumerable<KeyValuePair<byte[], byte[]>> pairs)
  {
    long added = 0, existing = 0;
    foreach (var (key, value) in pairs) {
      if (TryAdd(key, value)) {
        added++;
      }
      else {
        existing++;
      }
    }

    return (added, existing);
  }

  /// <inheritdoc />
  public bool TryRemove(byte[] key, [MaybeNullWhen(false)] out byte[] value)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (!TryGetValueUnlocked(key, hashes, out value)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        RemoveUnlocked(key);
      }

      return true;
    }
  }

  /// <inheritdoc />
  public long TryRemove(IEnumerable<byte[]> keys)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    if (keys is ICollection<byte[]> { Count: <= 3 } collection) {
      return collection.LongCount(key => TryRemove(key, out _));
    }

    long removed = 0;
    using (state.ReadWriteLock.AcquireWriteLock()) {
      foreach (var key in keys) {
        if (!ContainsKey(key)) {
          continue;
        }

        RemoveUnlocked(key);
        removed++;
      }

      return removed;
    }
  }

  /// <inheritdoc />
  public bool TryUpdate(byte[] key, byte[] newValue, byte[] comparisonValue)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireReadLock()) {
      if (!TryGetValueUnlocked(key, hashes, out var existingValue) ||
          !existingValue.AsSpan().SequenceEqual(comparisonValue)) {
        return false;
      }
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (!TryGetValueUnlocked(key, hashes, out var existingValue) ||
          !existingValue.AsSpan().SequenceEqual(comparisonValue)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return true;
      }
    }
  }

  /// <inheritdoc />
  public bool TryUpdate(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.TryUpdateFactory updateFactory)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireReadLock()) {
      if (!TryGetValueUnlocked(key, hashes, out _)) {
        return false;
      }
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (!TryGetValueUnlocked(key, hashes, out var existingValue)) {
        return false;
      }

      if (!updateFactory(key, existingValue, out var newValue)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return true;
      }
    }
  }

  /// <inheritdoc />
  public bool TryUpdate<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.TryUpdateFactoryWithArg<TArg> updateFactory,
    TArg arg)
  {
    if (Options.ReadOnly) {
      ThrowReadOnly();
    }

    var hashes = new BloomFilter.Hashes(key);
    using (state.ReadWriteLock.AcquireReadLock()) {
      if (!TryGetValueUnlocked(key, hashes, out _)) {
        return false;
      }
    }

    using (state.ReadWriteLock.AcquireUpgradableLock()) {
      if (!TryGetValueUnlocked(key, hashes, out var existingValue)) {
        return false;
      }

      if (!updateFactory(existingValue, arg, out var newValue)) {
        return false;
      }

      using (state.ReadWriteLock.AcquireWriteLock()) {
        PutValueUnlocked(key, newValue);

        return true;
      }
    }
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private bool ContainsKeyUnlocked(byte[] key, in BloomFilter.Hashes hashes)
  {
    if (!Options.ReadOnly && memoryTable.ContainsKey(key, hashes, out var removed)) {
      return !removed;
    }

    for (var index = 0; index < CurrentTableCount; index++) {
      if (tables[index].Value.ContainsKey(key, hashes, out removed)) {
        return !removed;
      }
    }

    return false;
  }

  private void FlushTableUnlocked(IReadableTable table)
  {
    var newId = state.AllocateIdentifier();
    var sst = state.FindFile(newId);

    using (var builder = new SSTableBuilder(
             new FileStream(
               sst.FullName,
               FileMode.CreateNew,
               FileAccess.Write,
               FileShare.None,
               1),
             state.Salt,
             Options)) {
      table.CopyTo(builder);
    }

    state.AddToLevel(family, 0x00, newId);
  }

  private void FlushUnlocked()
  {
    if (Options.ReadOnly || memoryTable.IsEmpty) {
      return;
    }

    // Flush the journal first, if any.
    state.Flush();

    FlushTableUnlocked(memoryTable);

    // At this point the data should be safely on disk, so throw away the journal
    state.ClearJournal();
    memoryTable = new MemoryTable(Options, unchecked(memoryTable.Generation + 1));
    ReopenSSTables();

    try {
      OnFlushMemoryTable?.Invoke(this, this);
    }
    catch {
      // ignored
    }

    if (allowMerge) {
      MaybeMerge();
    }
  }

  private IEnumerable<KeyValuePair<byte[], byte[]>> GetInternalEnumerable(bool readValues)
  {
    using var enumerator = GetInternalEnumerator(readValues);
    while (enumerator.MoveNext()) {
      yield return enumerator.Current;
    }
  }

  private MergeEnumerator GetInternalEnumerator(bool readValues)
  {
    using (state.ReadWriteLock.AcquireReadLock()) {
      var t = new List<IReadableTable>();
      if (!memoryTable.IsEmpty) {
        t.Add(memoryTable.Clone());
      }

      t.AddRange(tables.Select(i => i.Value));

      return new MergeEnumerator([.. t], readValues, Options.Comparer);
    }
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private void MaybeFlushMemoryTableUnlocked()
  {
    if (memoryTable.ApproxSize <= level0TargetSize &&
        state.JournalLength <= level0TargetSize * 25) {
      return;
    }

    FlushUnlocked();
  }

  private KeyValuePair<ulong, ISSTable> OpenSSTable(ulong id)
  {
    var file = state.FindFile(id);

    ISSTable OpenSSTableInternal()
    {
      var fileStream = new FileStream(
        file.FullName,
        FileMode.Open,
        FileAccess.Read,
        FileShare.Read,
        1,
        FileOptions.RandomAccess);
      var cache = blockCache.Get(id);
      try {
        return Options.KeyCacheMode switch {
          PlaneKeyCacheMode.NoKeyCaching => new SSTable(
            fileStream,
            state.Salt,
            cache,
            Options),
          PlaneKeyCacheMode.AutoKeyCaching => fileStream.Length < level0TargetSize
            ? new SSTableKeyCached(fileStream, state.Salt, cache, Options)
            : new SSTable(fileStream, state.Salt, cache, Options),
          PlaneKeyCacheMode.ForceKeyCaching => new SSTableKeyCached(
            fileStream,
            state.Salt,
            cache,
            Options),
          _ => throw new InvalidOperationException("Invalid key cache mode")
        };
      }
      catch {
        fileStream.Dispose();
        cache.Dispose();

        throw;
      }
    }

    try {
      var rv = new KeyValuePair<ulong, ISSTable>(id, OpenSSTableInternal());
      try {
        if (Options.RepairMode) {
          // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
          if (rv.Value.Enumerate().Any(kv => kv.Key == null)) {
            throw new IOException("Not properly enumerable");
          }
        }
      }
      catch {
        rv.Value.Dispose();

        throw;
      }

      backgroundQueue.Queue(rv.Value.EnsureLazyInit);

      return rv;
    }
    catch (Exception ex) when (Options.RepairMode) {
      {
        var ok = new List<KeyValuePair<byte[], byte[]?>>();
        try {
          using var broken = OpenSSTableInternal();
          foreach (var (key, _) in
                   // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
                   broken.EnumerateKeys().Where(kv => kv.Key != null)) {
            try {
              if (!broken.TryGet(key, new BloomFilter.Hashes(key), out var val)) {
                continue;
              }

              ok.Add(new KeyValuePair<byte[], byte[]?>(key, val));
            }
            catch {
              // ignored;
            }
          }
        }
        catch {
          // ignored;
        }

        using var builder = new SSTableBuilder(
          new FileStream(
            file.FullName,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            1),
          state.Salt,
          Options);
        foreach (var (key, value) in ok) {
          if (value == null) {
            builder.Remove(key);

            continue;
          }

          builder.Put(key, value);
        }
      }
      Options.RepairCallback?.Invoke(this, new PlaneRepairEventArgs(file, ex));

      return OpenSSTable(id);
    }
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private void PutLargeValue(byte[] key, byte[] value)
  {
    var mj = new JournalUniqueMemory();
    mj.Put(key, value);
    FlushTableUnlocked(mj);
    ReopenSSTables();
    if (allowMerge) {
      MaybeMerge();
    }
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  private void PutValueUnlocked(byte[] key, byte[] value)
  {
    if (value.Length >= largeValueSize &&
        !memoryTable.ContainsKey(key, new BloomFilter.Hashes(key), out _)) {
      PutLargeValue(key, value);

      return;
    }

    state.Put(key, value);
    memoryTable.Put(key, value);
    MaybeFlushMemoryTableUnlocked();
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  private void RemoveUnlocked(byte[] key)
  {
    state.Remove(key);
    memoryTable.Remove(key);
    MaybeFlushMemoryTableUnlocked();
  }

  private void ReopenSSTables()
  {
    var existing = tables.ToDictionary(i => i.Key, i => i.Value);

    tables = state.Sequence(family).Select(MaybeOpenSSTable).ToArray();
    CurrentTableCount = tables.Length;

    foreach (var oldTable in existing.Values) {
      oldTable.DeleteOnClose();
      oldTable.Dispose();
    }

    return;

    KeyValuePair<ulong, ISSTable> MaybeOpenSSTable(ulong id)
    {
      return existing.Remove(id, out var table)
        ? new KeyValuePair<ulong, ISSTable>(id, table)
        : OpenSSTable(id);
    }
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private bool TryGetValueFromMemoryUnlocked(
    byte[] key,
    in BloomFilter.Hashes hashes,
    [MaybeNullWhen(false)] out byte[] value)
  {
    if (memoryTable.TryGet(key, hashes, out var val) && val != null) {
      value = val;

      return true;
    }

    value = [];

    return false;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private bool TryGetValueUnlocked(
    byte[] key,
    in BloomFilter.Hashes hashes,
    [MaybeNullWhen(false)] out byte[] value)
  {
    byte[]? val;
    if (!Options.ReadOnly) {
      if (memoryTable.TryGet(key, hashes, out val)) {
        if (val == null) {
          value = [];

          return false;
        }

        value = val;

        return true;
      }
    }

    for (var index = 0; index < CurrentTableCount; index++) {
      if (!tables[index].Value.TryGet(key, hashes, out val)) {
        continue;
      }

      if (val == null) {
        value = [];

        return false;
      }

      value = val;

      return true;
    }

    value = null!;

    return false;
  }
}

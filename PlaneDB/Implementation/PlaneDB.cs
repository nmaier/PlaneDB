using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>Your byte[]-to-byte[] persistent key-value store!</summary>
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
  [SuppressMessage("ReSharper", "UseDeconstructionOnParameter")]
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  public sealed partial class PlaneDB : IPlaneDB<byte[], byte[]>
  {
    internal const int BASE_TARGET_SIZE = 8388608;

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

    private readonly BlockCache blockCache;
    private readonly PlaneDBOptions options;
    private readonly PlaneDBState state;
    private bool allowMerge = true;
    private int disposed;
    private long generation;
    private MemoryTable memoryTable;
    private KeyValuePair<ulong, SSTable>[] tables = Array.Empty<KeyValuePair<ulong, SSTable>>();
    private readonly byte[] family = Array.Empty<byte>();

    /// <inheritdoc />
    /// <param name="location">Directory that will store the PlaneDB</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    /// <summary>Opens or creates a new PlaneDB.</summary>
    public PlaneDB(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
    {
      options.Validate();

      Location = location;

      this.options = options.Clone();
      blockCache = new BlockCache(options.BlockCacheCapacity);
      memoryTable = new MemoryTable(options);

      if (mode == FileMode.CreateNew || mode == FileMode.OpenOrCreate) {
        location.Create();
      }

      state = new PlaneDBState(location, mode, options);

      ReopenSSTables();
      if (tables.Count(i => i.Value.DiskSize < 524288) > 2) {
        MaybeMerge(true);
      }

      if (!options.ThreadSafe) {
        return;
      }

      mergeThread = new Thread(MergeLoop) { Priority = ThreadPriority.BelowNormal, Name = "Plane-Background-Merge" };
      mergeThread.Start();
    }

    /// <summary>
    ///   All levels and corresponding identifiers in this DB
    /// </summary>
    public SortedList<byte, ulong[]> AllLevels => state.Manifest.GetAllLevels(family);

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
      memoryTable = new MemoryTable(options);
      state.Manifest.Clear();
      ReopenSSTables();
    }

    /// <inheritdoc />
    public bool Contains(KeyValuePair<byte[], byte[]> item)
    {
      return ContainsKey(item.Key);
    }

    /// <inheritdoc />
    public void CopyTo(KeyValuePair<byte[], byte[]>[] array, int arrayIndex)
    {
      this.ToArray().CopyTo(array, arrayIndex);
    }

    /// <inheritdoc />
    public int Count => GetInternalEnumerable(false).Count();

    /// <inheritdoc />
    public bool IsReadOnly => false;

    /// <inheritdoc />
    public bool Remove(KeyValuePair<byte[], byte[]> item)
    {
      return Remove(item.Key);
    }

    /// <inheritdoc />
    public void Add(byte[] key, byte[] value)
    {
      if (!TryAdd(key, value)) {
        ThrowKeyExists();
      }
    }

    /// <inheritdoc />
    public bool ContainsKey(byte[] key)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        return ContainsKeyUnlocked(key);
      }
      finally {
        readWriteLock.ExitReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] this[byte[] key]
    {
      get
      {
        if (!TryGetValue(key, out var val)) {
          ThrowKeyNotFoundException();
        }

        return val;
      }
      set => Set(key, value);
    }

    /// <inheritdoc />
    public ICollection<byte[]> Keys => GetInternalEnumerable(false).Select(i => i.Key).ToArray();

    /// <inheritdoc />
    public bool Remove(byte[] key)
    {
      return TryRemove(key, out _);
    }

    /// <inheritdoc />
    public bool TryGetValue(byte[] key, [NotNullWhen(true)] out byte[] value)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        return TryGetValueUnlocked(key, out value);
      }
      finally {
        readWriteLock.ExitReadLock();
      }
    }

    /// <inheritdoc />
    public ICollection<byte[]> Values => GetInternalEnumerable(true).Select(i => i.Value).ToArray();

    /// <inheritdoc />
    public void Dispose()
    {
      if (Interlocked.CompareExchange(ref disposed, 1, 0) != 0) {
        throw new ObjectDisposedException(nameof(PlaneDB));
      }

      Flush();
      mergeRequests.CompleteAdding();
      mergeThread?.Join();

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
    public void Compact()
    {
      if (CurrentTableCount <= 1) {
        return;
      }

      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterWriteLock();
      try {
        allowMerge = false;
        try {
          FlushUnlocked();
        }
        finally {
          allowMerge = true;
        }

        CompactLevels();
        state.MaybeCompactManifest();
      }
      finally {
        readWriteLock.ExitWriteLock();
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
    public int CurrentTableCount => tables.Length;

    /// <inheritdoc />
    public void Flush()
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterWriteLock();
      try {
        FlushUnlocked();
      }
      finally {
        readWriteLock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public DirectoryInfo Location { get; }

    /// <inheritdoc />
    public void MassInsert(Action action)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterWriteLock();
      allowMerge = false;
      try {
        action();
      }
      finally {
        allowMerge = true;
        FlushUnlocked();
        MaybeMerge();
        readWriteLock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public string TableSpace => options.TableSpace;

    /// <inheritdoc />
    public byte[] AddOrUpdate(byte[] key, Func<byte[], byte[]> addValueFactory,
      Func<byte[], byte[], byte[]> updateValueFactory)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterUpgradeableReadLock();
      try {
        byte[] newValue;
        if (TryGetValueUnlocked(key, out var existingValue)) {
          newValue = updateValueFactory(key, existingValue);
          if (options.Comparer.Equals(newValue, existingValue)) {
            return newValue;
          }
        }
        else {
          newValue = addValueFactory(key);
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] AddOrUpdate(byte[] key, byte[] addValue, Func<byte[], byte[], byte[]> updateValueFactory)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          addValue = updateValueFactory(key, existingValue);
          if (options.Comparer.Equals(addValue, existingValue)) {
            return addValue;
          }
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, addValue);
          memoryTable.Put(key, addValue);
          MaybeFlushMemoryTable();
          return addValue;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] AddOrUpdate<TArg>(byte[] key, Func<byte[], TArg, byte[]> addValueFactory,
      Func<byte[], byte[], TArg, byte[]> updateValueFactory, TArg factoryArgument)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterUpgradeableReadLock();
      try {
        byte[] newValue;
        if (TryGetValueUnlocked(key, out var existingValue)) {
          newValue = updateValueFactory(key, existingValue, factoryArgument);
          if (options.Comparer.Equals(newValue, existingValue)) {
            return newValue;
          }
        }
        else {
          newValue = addValueFactory(key, factoryArgument);
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public void CopyTo(IDictionary<byte[], byte[]> destination)
    {
      if (destination is PlaneDB other) {
        other.MassInsert(() => {
          foreach (var kv in this) {
            other.Set(kv.Key, kv.Value);
          }
        });
        return;
      }

      foreach (var kv in this) {
        destination[kv.Key] = kv.Value;
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd(byte[] key, Func<byte[], byte[]> valueFactory)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      long last;
      try {
        if (TryGetValueUnlocked(key, out var value)) {
          return value;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (last == generation) {
          if (TryGetValueFromMemoryUnlocked(key, out var value)) {
            return value;
          }
        }
        else if (TryGetValueUnlocked(key, out var value)) {
          return value;
        }

        var newValue = valueFactory(key);
        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd(byte[] key, byte[] value)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (last == generation) {
          if (TryGetValueFromMemoryUnlocked(key, out var existingValue)) {
            return existingValue;
          }
        }
        else if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return value;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd(byte[] key, byte[] value, out bool added)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          added = false;
          return existingValue;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (last == generation) {
          if (TryGetValueFromMemoryUnlocked(key, out var existingValue)) {
            added = false;
            return existingValue;
          }
        }
        else if (TryGetValueUnlocked(key, out var existingValue)) {
          added = false;
          return existingValue;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          added = true;
          return value;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd<TArg>(byte[] key, Func<byte[], TArg, byte[]> valueFactory, TArg factoryArgument)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (last == generation) {
          if (TryGetValueFromMemoryUnlocked(key, out var value)) {
            return value;
          }
        }
        else if (TryGetValueUnlocked(key, out var value)) {
          return value;
        }

        var newValue = valueFactory(key, factoryArgument);
        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public IEnumerable<byte[]> KeysIterator => GetInternalEnumerable(false).Select(i => i.Key);

    /// <inheritdoc />
    public event EventHandler<IPlaneDB<byte[], byte[]>>? OnFlushMemoryTable;

    /// <inheritdoc />
    public event EventHandler<IPlaneDB<byte[], byte[]>>? OnMergedTables;

    /// <inheritdoc />
    public void Set(byte[] key, byte[] value)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterWriteLock();
      try {
        state.Journal.Put(key, value);
        memoryTable.Put(key, value);
        MaybeFlushMemoryTable();
      }
      finally {
        readWriteLock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public bool TryAdd(byte[] key, byte[] value)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (ContainsKeyUnlocked(key)) {
          return false;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (generation == last) {
          if (memoryTable.ContainsKey(key, out var removed) && !removed) {
            return false;
          }
        }
        else if (ContainsKeyUnlocked(key)) {
          return false;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryAdd(byte[] key, byte[] value, [NotNullWhen(true)] out byte[] existing)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out existing)) {
          return false;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (generation == last) {
          if (TryGetValueFromMemoryUnlocked(key, out existing)) {
            return false;
          }
        }
        else if (TryGetValueUnlocked(key, out existing)) {
          return false;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryRemove(byte[] key, [NotNullWhen(true)] out byte[] value)
    {
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (!TryGetValueUnlocked(key, out value)) {
          return false;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Remove(key);
          memoryTable.Remove(key);
          MaybeFlushMemoryTable();
        }
        finally {
          readWriteLock.ExitWriteLock();
        }

        return true;
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryUpdate(byte[] key, byte[] newValue, byte[] comparisonValue)
    {
      long last;
      var readWriteLock = state.ReadWriteLock;
      readWriteLock.EnterReadLock();
      try {
        if (!TryGetValueUnlocked(key, out var existingValue) ||
            !options.Comparer.Equals(existingValue, comparisonValue)) {
          return false;
        }

        last = generation;
      }
      finally {
        readWriteLock.ExitReadLock();
      }

      readWriteLock.EnterUpgradeableReadLock();
      try {
        if (generation == last) {
          if (!TryGetValueFromMemoryUnlocked(key, out var existingValue) ||
              !options.Comparer.Equals(existingValue, comparisonValue)) {
            return false;
          }
        }
        else if (!TryGetValueUnlocked(key, out var existingValue) ||
                 !options.Comparer.Equals(existingValue, comparisonValue)) {
          return false;
        }

        readWriteLock.EnterWriteLock();
        try {
          state.Journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          readWriteLock.ExitWriteLock();
        }
      }
      finally {
        readWriteLock.ExitUpgradeableReadLock();
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool ContainsKeyUnlocked(byte[] key)
    {
      if (memoryTable.ContainsKey(key, out var removed)) {
        return !removed;
      }

      if (tables.Any(t => t.Value.ContainsKey(key, out removed))) {
        return !removed;
      }

      return false;
    }

    internal void FlushUnlocked()
    {
      if (memoryTable.IsEmpty) {
        return;
      }

      var newId = state.Manifest.AllocateIdentifier();
      var sst = state.Manifest.FindFile(newId);

      using (var builder =
        new SSTableBuilder(new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                           options)) {
        memoryTable.CopyTo(builder);
      }

      state.Manifest.AddToLevel(family, 0x00, newId);

      // At this point the data should be safely on disk, so throw away the journal
      state.ClearJournal();
      memoryTable = new MemoryTable(options);
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

      generation++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetValueFromMemoryUnlocked(byte[] key, [NotNullWhen(true)] out byte[] value)
    {
      if (memoryTable.TryGet(key, out var val) && val != null) {
        value = val;
        return true;
      }

      value = Array.Empty<byte>();
      return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetValueUnlocked(byte[] key, [NotNullWhen(true)] out byte[] value)
    {
      if (memoryTable.TryGet(key, out var val)) {
        if (val == null) {
          value = Array.Empty<byte>();
          return false;
        }

        value = val;
        return true;
      }

      if (tables.Any(t => t.Value.TryGet(key, out val))) {
        if (val == null) {
          value = Array.Empty<byte>();
          return false;
        }

        value = val;
        return true;
      }

      value = Array.Empty<byte>();
      return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MaybeFlushMemoryTable()
    {
      if (memoryTable.ApproxSize <= BASE_TARGET_SIZE && state.Journal.Length <= BASE_TARGET_SIZE * 5) {
        return;
      }

      FlushUnlocked();
    }

    private KeyValuePair<ulong, SSTable> OpenSSTable(ulong id)
    {
      var file = state.Manifest.FindFile(id);
      return new KeyValuePair<ulong, SSTable>(id, new SSTable(
                                                new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                                                               FileShare.Read, 1),
                                                blockCache.Get(id), options));
    }

    private void ReopenSSTables()
    {
      var existing = tables.ToDictionary(i => i.Key, i => i.Value);

      KeyValuePair<ulong, SSTable> MaybeOpenSSTable(ulong id)
      {
        return existing.Remove(id, out var table) ? new KeyValuePair<ulong, SSTable>(id, table) : OpenSSTable(id);
      }

      tables = state.Manifest.Sequence(family).AsParallel().AsOrdered().WithDegreeOfParallelism(4).Select(MaybeOpenSSTable)
        .ToArray();
      foreach (var kv in existing) {
        kv.Value.Dispose();
        try {
          state.Manifest.FindFile(kv.Key).Delete();
        }
        catch {
          // ignored here
        }
      }
    }
  }
}
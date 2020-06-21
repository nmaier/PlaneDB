using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
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
    private const int BASE_TARGET_SIZE = 8388608;
    private const string JOURNAL_FILE = "JOURNAL";
    private const string LOCK_FILE = "LOCK";
    private const string MANIFEST_FILE = "MANIFEST";

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
    private readonly Stream lockFile;
    private readonly PlaneDBOptions options;
    private readonly IReadWriteLock rwlock;
    private bool allowMerge = true;
    private int disposed;
    private long generation;
    private IJournal journal;
    private Manifest manifest;
    private MemoryTable memoryTable;
    private KeyValuePair<ulong, SSTable>[] tables = Array.Empty<KeyValuePair<ulong, SSTable>>();

    /// <inheritdoc />
    /// <param name="location">Directory that will store the PlaneDB</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    /// <summary>Opens or creates a new PlaneDB.</summary>
    public PlaneDB(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
    {
      options.Validate();

      Location = location;
      rwlock = options.ThreadSafe ? options.TrueReadWriteLock : new FakeReadWriteLock();

      this.options = options.Clone();
      blockCache = new BlockCache(options.BlockCacheCapacity);
      memoryTable = new MemoryTable(options);

      if (mode == FileMode.CreateNew || mode == FileMode.OpenOrCreate) {
        location.Create();
      }

      try {
        lockFile = mode switch {
          FileMode.CreateNew => new FileStream(FindFile(LOCK_FILE).FullName, FileMode.CreateNew, FileAccess.ReadWrite,
                                               FileShare.None),
          FileMode.Open => new FileStream(FindFile(LOCK_FILE).FullName, FileMode.Create, FileAccess.ReadWrite,
                                          FileShare.None),
          FileMode.OpenOrCreate => new FileStream(FindFile(LOCK_FILE).FullName, FileMode.Create, FileAccess.ReadWrite,
                                                  FileShare.None),
          _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };
      }
      catch (UnauthorizedAccessException ex) {
        throw new AlreadyLockedException(ex);
      }
      catch (IOException ex) {
        throw new AlreadyLockedException(ex);
      }

      // ReSharper disable once ConvertSwitchStatementToSwitchExpression
      switch (mode) {
        case FileMode.CreateNew:
        case FileMode.Open:
        case FileMode.OpenOrCreate:
          manifest = new Manifest(new FileStream(FindFile(MANIFEST_FILE).FullName, mode, FileAccess.ReadWrite,
                                                 FileShare.None, 4096), options);
          break;
        case FileMode.Append:
        case FileMode.Create:
        case FileMode.Truncate:
          throw new NotSupportedException(nameof(mode));
        default:
          throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
      }

      RemoveOrphans();
      MaybeReplayJournal();

      journal = OpenJournal();
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
    public SortedList<byte, ulong[]> AllLevels => manifest.AllLevels;

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
      journal.Dispose();
      journal = OpenJournal();
      manifest.Clear();
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
      rwlock.EnterReadLock();
      try {
        return ContainsKeyUnlocked(key);
      }
      finally {
        rwlock.ExitReadLock();
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
    public bool TryGetValue(byte[] key, out byte[] value)
    {
      rwlock.EnterReadLock();
      try {
        return TryGetValueUnlocked(key, out value);
      }
      finally {
        rwlock.ExitReadLock();
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

      journal.Dispose();

      MaybeCompactManifest();
      manifest.Dispose();
      blockCache.Dispose();
      lockFile.Dispose();
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

      rwlock.EnterWriteLock();
      try {
        allowMerge = false;
        try {
          FlushUnlocked();
        }
        finally {
          allowMerge = true;
        }

        CompactLevels();
        MaybeCompactManifest();
      }
      finally {
        rwlock.ExitWriteLock();
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
      rwlock.EnterWriteLock();
      try {
        FlushUnlocked();
      }
      finally {
        rwlock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public DirectoryInfo Location { get; }

    /// <inheritdoc />
    public void MassInsert(Action action)
    {
      rwlock.EnterWriteLock();
      allowMerge = false;
      try {
        action();
      }
      finally {
        allowMerge = true;
        FlushUnlocked();
        MaybeMerge();
        rwlock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public string TableSpace => options.TableSpace;

    /// <inheritdoc />
    public byte[] AddOrUpdate(byte[] key, Func<byte[], byte[]> addValueFactory,
      Func<byte[], byte[], byte[]> updateValueFactory)
    {
      rwlock.EnterUpgradeableReadLock();
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

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] AddOrUpdate(byte[] key, byte[] addValue, Func<byte[], byte[], byte[]> updateValueFactory)
    {
      rwlock.EnterUpgradeableReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          addValue = updateValueFactory(key, existingValue);
          if (options.Comparer.Equals(addValue, existingValue)) {
            return addValue;
          }
        }

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, addValue);
          memoryTable.Put(key, addValue);
          MaybeFlushMemoryTable();
          return addValue;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] AddOrUpdate<TArg>(byte[] key, Func<byte[], TArg, byte[]> addValueFactory,
      Func<byte[], byte[], TArg, byte[]> updateValueFactory, TArg factoryArgument)
    {
      rwlock.EnterUpgradeableReadLock();
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

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
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
      rwlock.EnterReadLock();
      long last;
      try {
        if (TryGetValueUnlocked(key, out var value)) {
          return value;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
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
        rwlock.EnterWriteLock();
        try {
          journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd(byte[] key, byte[] value)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
      try {
        if (last == generation) {
          if (TryGetValueFromMemoryUnlocked(key, out var existingValue)) {
            return existingValue;
          }
        }
        else if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return value;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd(byte[] key, byte[] value, out bool added)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          added = false;
          return existingValue;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
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

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          added = true;
          return value;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public byte[] GetOrAdd<TArg>(byte[] key, Func<byte[], TArg, byte[]> valueFactory, TArg factoryArgument)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out var existingValue)) {
          return existingValue;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
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
        rwlock.EnterWriteLock();
        try {
          journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return newValue;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
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
      rwlock.EnterWriteLock();
      try {
        journal.Put(key, value);
        memoryTable.Put(key, value);
        MaybeFlushMemoryTable();
      }
      finally {
        rwlock.ExitWriteLock();
      }
    }

    /// <inheritdoc />
    public bool TryAdd(byte[] key, byte[] value)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (ContainsKeyUnlocked(key)) {
          return false;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
      try {
        if (generation == last) {
          if (memoryTable.ContainsKey(key, out var removed) && !removed) {
            return false;
          }
        }
        else if (ContainsKeyUnlocked(key)) {
          return false;
        }

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryAdd(byte[] key, byte[] value, out byte[] existing)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (TryGetValueUnlocked(key, out existing)) {
          return false;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
      try {
        if (generation == last) {
          if (TryGetValueFromMemoryUnlocked(key, out existing)) {
            return false;
          }
        }
        else if (TryGetValueUnlocked(key, out existing)) {
          return false;
        }

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, value);
          memoryTable.Put(key, value);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryRemove(byte[] key, out byte[] value)
    {
      rwlock.EnterUpgradeableReadLock();
      try {
        if (!TryGetValueUnlocked(key, out value)) {
          return false;
        }

        rwlock.EnterWriteLock();
        try {
          journal.Remove(key);
          memoryTable.Remove(key);
          MaybeFlushMemoryTable();
        }
        finally {
          rwlock.ExitWriteLock();
        }

        return true;
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
      }
    }

    /// <inheritdoc />
    public bool TryUpdate(byte[] key, byte[] newValue, byte[] comparisonValue)
    {
      long last;
      rwlock.EnterReadLock();
      try {
        if (!TryGetValueUnlocked(key, out var existingValue) ||
            !options.Comparer.Equals(existingValue, comparisonValue)) {
          return false;
        }

        last = generation;
      }
      finally {
        rwlock.ExitReadLock();
      }

      rwlock.EnterUpgradeableReadLock();
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

        rwlock.EnterWriteLock();
        try {
          journal.Put(key, newValue);
          memoryTable.Put(key, newValue);
          MaybeFlushMemoryTable();
          return true;
        }
        finally {
          rwlock.ExitWriteLock();
        }
      }
      finally {
        rwlock.ExitUpgradeableReadLock();
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

      var newId = manifest.AllocateIdentifier();
      var sst = FindFile(newId);

      using (var builder =
        new SSTableBuilder(new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                           options)) {
        memoryTable.CopyTo(builder);
      }

      manifest.AddToLevel(0x00, newId);

      // At this point the data should be safely on disk, so throw away the journal
      journal.Dispose();
      journal = OpenJournal();
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
    internal bool TryGetValueFromMemoryUnlocked(byte[] key, out byte[] value)
    {
      if (memoryTable.TryGet(key, out var val) && val != null) {
        value = val;
        return true;
      }

      value = Array.Empty<byte>();
      return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetValueUnlocked(byte[] key, out byte[] value)
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

    private FileInfo FindFile(string filename)
    {
      var ts = string.IsNullOrEmpty(options.TableSpace) ? "default" : options.TableSpace;
      return new FileInfo(Path.Combine(Location.FullName, $"{ts}-{filename}.planedb"));
    }

    private FileInfo FindFile(ulong id)
    {
      return FindFile($"{id:D4}");
    }

    private void MaybeCompactManifest()
    {
      if (manifest.IsEmpty) {
        return;
      }

      var man = FindFile(MANIFEST_FILE);
      var newman = FindFile(MANIFEST_FILE + "-NEW");
      var oldman = FindFile(MANIFEST_FILE + "-OLD");
      manifest.Compact(new FileStream(newman.FullName, FileMode.Create, FileAccess.ReadWrite,
                                      FileShare.None, 4096));
      manifest.Dispose();
      File.Move(man.FullName, oldman.FullName);
      File.Move(newman.FullName, man.FullName);
      manifest = new Manifest(new FileStream(man.FullName, FileMode.Open, FileAccess.ReadWrite,
                                             FileShare.None, 4096), options);
      File.Delete(oldman.FullName);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void MaybeFlushMemoryTable()
    {
      if (memoryTable.ApproxSize <= BASE_TARGET_SIZE && journal.Length <= BASE_TARGET_SIZE * 5) {
        return;
      }

      FlushUnlocked();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MaybeReplayJournal()
    {
      if (!options.JournalEnabled) {
        return;
      }

      using var jbs =
        new FileStream(FindFile(JOURNAL_FILE).FullName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.None, 16384);
      if (jbs.Length <= 0 || jbs.Length == 4) {
        return;
      }

      var newId = manifest.AllocateIdentifier();
      var sst = FindFile(newId);

      try {
        using var builder =
          new SSTableBuilder(new FileStream(sst.FullName, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1),
                             options);
        Journal.ReplayOnto(jbs, options, builder);
        manifest.AddToLevel(0x00, newId);
      }
      catch (BrokenJournalException) {
        try {
          sst.Delete();
        }
        catch {
          // ignored
        }

        if (!options.AllowSkippingOfBrokenJournal) {
          throw;
        }
      }
    }

    private IJournal OpenJournal()
    {
      if (options.JournalEnabled) {
        return new Journal(new FileStream(FindFile(JOURNAL_FILE).FullName, FileMode.Create, FileAccess.ReadWrite,
                                          FileShare.None, BASE_TARGET_SIZE, FileOptions.SequentialScan), options);
      }

      return new FakeJournal();
    }

    private KeyValuePair<ulong, SSTable> OpenSSTable(ulong id)
    {
      var file = FindFile(id);
      return new KeyValuePair<ulong, SSTable>(id, new SSTable(
                                                new FileStream(file.FullName, FileMode.Open, FileAccess.Read,
                                                               FileShare.Read, 1),
                                                blockCache.Get(id), options));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void RemoveOrphans()
    {
      IEnumerable<FileInfo> FindOrphans()
      {
        var valid = manifest.Sequence().ToLookup(i => i);
        var ts = string.IsNullOrEmpty(options.TableSpace) ? "default" : options.TableSpace;
        var needle = new Regex($"{Regex.Escape(options.TableSpace)}-(.*)\\.planedb", RegexOptions.Compiled);
        foreach (var fi in Location.GetFiles($"{ts}-*.planedb", SearchOption.TopDirectoryOnly)) {
          var m = needle.Match(fi.Name);
          if (!m.Success) {
            continue;
          }

          var name = m.Groups[1].Value;
          if (!ulong.TryParse(name, out var id)) {
            switch (name) {
              case JOURNAL_FILE:
              case LOCK_FILE:
              case MANIFEST_FILE:
                break;
              default:
                yield return fi;
                break;
            }

            continue;
          }

          if (valid.Contains(id)) {
            continue;
          }

          yield return fi;
        }
      }

      var orphans = FindOrphans().ToArray();
      foreach (var orphan in orphans) {
        try {
          orphan.Delete();
        }
        catch {
          // ignored
        }
      }
    }

    private void ReopenSSTables()
    {
      var existing = tables.ToDictionary(i => i.Key, i => i.Value);

      KeyValuePair<ulong, SSTable> MaybeOpenSSTable(ulong id)
      {
        return existing.Remove(id, out var table) ? new KeyValuePair<ulong, SSTable>(id, table) : OpenSSTable(id);
      }

      tables = manifest.Sequence().AsParallel().AsOrdered().WithDegreeOfParallelism(4).Select(MaybeOpenSSTable)
        .ToArray();
      foreach (var kv in existing) {
        kv.Value.Dispose();
        try {
          FindFile(kv.Key).Delete();
        }
        catch {
          // ignored here
        }
      }
    }
  }
}
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
///   A simple Key-Value store
/// </summary>
[PublicAPI]
public class TypedPlaneDB<TKey, TValue> : IPlaneDB<TKey, TValue> where TKey : notnull
{
  private readonly IPlaneSerializer<TKey> keySerializer;

  private readonly
    Dictionary<IPlaneDBMergeParticipant<TKey, TValue>, ParticipantWrapper<TKey, TValue>>
    participants = [];

  private readonly IPlaneSerializer<TValue> valueSerializer;

  /// <summary>
  ///   Create a new typed Key-Value store
  /// </summary>
  /// <remarks>
  ///   Please note that the internal sort order will still be based upon the byte-array comparer
  /// </remarks>
  /// <param name="keySerializer">Serializer to use to handle keys</param>
  /// <param name="valueSerializer">Serializer to use to handle values</param>
  /// <param name="location">Directory that will store the PlaneDB</param>
  /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
  public TypedPlaneDB(
    IPlaneSerializer<TKey> keySerializer,
    IPlaneSerializer<TValue> valueSerializer,
    DirectoryInfo location,
    PlaneOptions options)
  {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    BaseDB = new PlaneDB(location, options);
    BaseDB.OnFlushMemoryTable += (_, _) => OnFlushMemoryTable?.Invoke(this, this);
    BaseDB.OnMergedTables += (_, _) => OnMergedTables?.Invoke(this, this);
  }

  internal TypedPlaneDB(
    IPlaneSerializer<TKey> keySerializer,
    IPlaneSerializer<TValue> valueSerializer,
    IPlaneDB<byte[], byte[]> baseDB)
  {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    BaseDB = baseDB;
    BaseDB.OnFlushMemoryTable += (_, _) => OnFlushMemoryTable?.Invoke(this, this);
    BaseDB.OnMergedTables += (_, _) => OnMergedTables?.Invoke(this, this);
  }

  /// <inheritdoc />
  public async IAsyncEnumerator<KeyValuePair<TKey, TValue>> GetAsyncEnumerator(
    CancellationToken cancellationToken = new())
  {
    var e = BaseDB.GetAsyncEnumerator(cancellationToken);
    while (await e.MoveNextAsync().ConfigureAwait(false)) {
      yield return new KeyValuePair<TKey, TValue>(
        keySerializer.Deserialize(e.Current.Key),
        valueSerializer.Deserialize(e.Current.Value));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Add(KeyValuePair<TKey, TValue> item)
  {
    BaseDB.Add(keySerializer.Serialize(item.Key), valueSerializer.Serialize(item.Value));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Clear()
  {
    BaseDB.Clear();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Contains(KeyValuePair<TKey, TValue> item)
  {
    return ContainsKey(item.Key);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  [CollectionAccess(CollectionAccessType.Read)]
  public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
  {
    foreach (var kv in this) {
      array[arrayIndex++] = kv;
    }
  }

  /// <inheritdoc />
  public int Count => BaseDB.Count;

  /// <inheritdoc />
  public bool IsReadOnly => BaseDB.IsReadOnly;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(KeyValuePair<TKey, TValue> item)
  {
    return BaseDB.Remove(keySerializer.Serialize(item.Key));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Add(TKey key, TValue value)
  {
    BaseDB.Add(keySerializer.Serialize(key), valueSerializer.Serialize(value));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool ContainsKey(TKey key)
  {
    return BaseDB.ContainsKey(keySerializer.Serialize(key));
  }

  /// <inheritdoc />
  public TValue this[TKey key]
  {
    get => valueSerializer.Deserialize(BaseDB[keySerializer.Serialize(key)]);
    set =>
      BaseDB.SetValue(keySerializer.Serialize(key), valueSerializer.Serialize(value));
  }

  /// <inheritdoc />
  public ICollection<TKey> Keys =>
    BaseDB.Keys.Select(k => keySerializer.Deserialize(k)).ToArray();

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(TKey key)
  {
    return BaseDB.Remove(keySerializer.Serialize(key));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryGetValue(TKey key, out TValue value)
  {
    if (BaseDB.TryGetValue(keySerializer.Serialize(key), out var raw)) {
      value = valueSerializer.Deserialize(raw);

      return true;
    }

    value = default!;

    return false;
  }

  /// <inheritdoc />
  public ICollection<TValue> Values =>
    BaseDB.Values.Select(v => valueSerializer.Deserialize(v)).ToArray();

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Dispose()
  {
    Dispose(true);
    GC.SuppressFinalize(this);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  [MustDisposeResource]
  IEnumerator IEnumerable.GetEnumerator()
  {
    return ((IEnumerable)BaseDB).GetEnumerator();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  [MustDisposeResource]
  public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
  {
    return Stream().GetEnumerator();

    IEnumerable<KeyValuePair<TKey, TValue>> Stream()
    {
      foreach (var kv in BaseDB) {
        yield return new KeyValuePair<TKey, TValue>(
          keySerializer.Deserialize(kv.Key),
          valueSerializer.Deserialize(kv.Value));
      }
    }
  }

  /// <inheritdoc />
  public IPlaneDB<byte[], byte[]> BaseDB { get; }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Compact(CompactionMode mode = CompactionMode.Normal)
  {
    BaseDB.Compact(mode);
  }

  /// <inheritdoc />
  public long CurrentBloomBits => BaseDB.CurrentBloomBits;

  /// <inheritdoc />
  public long CurrentDiskSize => BaseDB.CurrentDiskSize;

  /// <inheritdoc />
  public long CurrentIndexBlockCount => BaseDB.CurrentIndexBlockCount;

  /// <inheritdoc />
  public long CurrentRealSize => BaseDB.CurrentRealSize;

  /// <inheritdoc />
  public int CurrentTableCount => BaseDB.CurrentTableCount;

  /// <inheritdoc />
  public void Flush()
  {
    BaseDB.Flush();
  }

  /// <inheritdoc />
  public DirectoryInfo Location => BaseDB.Location;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void MassInsert([InstantHandle] Action action)
  {
    BaseDB.MassInsert(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TResult MassInsert<TResult>(Func<TResult> action)
  {
    return BaseDB.MassInsert(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void MassRead(Action action)
  {
    BaseDB.MassRead(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TResult MassRead<TResult>(Func<TResult> action)
  {
    return BaseDB.MassRead(action);
  }

  /// <inheritdoc />
  public PlaneOptions Options => BaseDB.Options;

  /// <inheritdoc />
  public string TableSpace => BaseDB.TableSpace;

  /// <inheritdoc />
  public event EventHandler<IPlaneDB<TKey, TValue>>? OnFlushMemoryTable;

  // XXX
  /// <inheritdoc />
  public event EventHandler<IPlaneDB<TKey, TValue>>? OnMergedTables;

  /// <inheritdoc />
  public void RegisterMergeParticipant(IPlaneDBMergeParticipant<TKey, TValue> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.UnregisterMergeParticipant(old);
      }

      var wrapped = new ParticipantWrapper<TKey, TValue>(
        keySerializer,
        valueSerializer,
        participant);
      participants.Add(participant, wrapped);
      BaseDB.RegisterMergeParticipant(wrapped);
    }
  }

  /// <inheritdoc />
  public void UnregisterMergeParticipant(
    IPlaneDBMergeParticipant<TKey, TValue> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.UnregisterMergeParticipant(old);
      }
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue AddOrUpdate(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactory addValueFactory,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.UpdateValueFactory updateValueFactory)
  {
    return valueSerializer.Deserialize(
      BaseDB.AddOrUpdate(
        keySerializer.Serialize(key),
        AddValueFactory,
        UpdateValueFactory));

    byte[] AddValueFactory()
    {
      return valueSerializer.Serialize(addValueFactory());
    }

    byte[] UpdateValueFactory(in byte[] raw)
    {
      return valueSerializer.Serialize(
        updateValueFactory(valueSerializer.Deserialize(raw)));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue AddOrUpdate(
    TKey key,
    TValue addValue,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.UpdateValueFactory updateValueFactory)
  {
    return valueSerializer.Deserialize(
      BaseDB.AddOrUpdate(
        keySerializer.Serialize(key),
        valueSerializer.Serialize(addValue),
        UpdateValueFactory));

    byte[] UpdateValueFactory(in byte[] raw)
    {
      return valueSerializer.Serialize(
        updateValueFactory(valueSerializer.Deserialize(raw)));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue AddOrUpdate<TArg>(
    TKey key,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.ValueFactoryWithArg<TArg> addValueFactory,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.UpdateValueFactoryWithArg<TArg> updateValueFactory,
    TArg factoryArgument)
  {
    return valueSerializer.Deserialize(
      BaseDB.AddOrUpdate(
        keySerializer.Serialize(key),
        AddValueFactory,
        UpdateValueFactory,
        factoryArgument));

    byte[] AddValueFactory(TArg arg)
    {
      return valueSerializer.Serialize(addValueFactory(arg));
    }

    byte[] UpdateValueFactory(in byte[] raw, TArg arg)
    {
      return valueSerializer.Serialize(
        updateValueFactory(valueSerializer.Deserialize(raw), arg));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void CopyTo(IDictionary<TKey, TValue> destination)
  {
    switch (destination) {
      case TypedPlaneDB<TKey, TValue> pother
        when pother.keySerializer.GetType() == keySerializer.GetType() &&
             pother.valueSerializer.GetType() == valueSerializer.GetType():
        BaseDB.CopyTo(pother.BaseDB);

        return;
      default:
        foreach (var kv in this) {
          destination[kv.Key] = kv.Value;
        }

        return;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<TKey> GetKeysIteratorAsync(
    [EnumeratorCancellation] CancellationToken token)
  {
    await foreach (var key in BaseDB.GetKeysIteratorAsync(token).ConfigureAwait(false)) {
      yield return keySerializer.Deserialize(key);
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue GetOrAdd(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactory valueFactory)
  {
    return valueSerializer.Deserialize(
      BaseDB.GetOrAdd(keySerializer.Serialize(key), Factory));

    byte[] Factory()
    {
      return valueSerializer.Serialize(valueFactory());
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue GetOrAdd(TKey key, TValue value)
  {
    return valueSerializer.Deserialize(
      BaseDB.GetOrAdd(keySerializer.Serialize(key), valueSerializer.Serialize(value)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue GetOrAdd(TKey key, TValue value, out bool added)
  {
    return valueSerializer.Deserialize(
      BaseDB.GetOrAdd(
        keySerializer.Serialize(key),
        valueSerializer.Serialize(value),
        out added));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TValue GetOrAdd<TArg>(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactoryWithArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    return valueSerializer.Deserialize(
      BaseDB.GetOrAdd(keySerializer.Serialize(key), Factory, factoryArgument));

    byte[] Factory(TArg arg)
    {
      return valueSerializer.Serialize(valueFactory(arg));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<KeyValuePair<TKey, TValue>> keysAndDefaults)
  {
    return BaseDB
      .GetOrAddRange(
        keysAndDefaults.Select(
          i => new KeyValuePair<byte[], byte[]>(
            keySerializer.Serialize(i.Key),
            valueSerializer.Serialize(i.Value))))
      .Select(
        i => new KeyValuePair<TKey, TValue>(
          keySerializer.Deserialize(i.Key),
          valueSerializer.Deserialize(i.Value)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    TValue value)
  {
    return BaseDB
      .GetOrAddRange(
        keys.Select(i => keySerializer.Serialize(i)),
        valueSerializer.Serialize(value))
      .Select(
        i => new KeyValuePair<TKey, TValue>(
          keySerializer.Deserialize(i.Key),
          valueSerializer.Deserialize(i.Value)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactoryWithKey valueFactory)
  {
    return BaseDB.GetOrAddRange(keys.Select(i => keySerializer.Serialize(i)), Factory)
      .Select(
        i => new KeyValuePair<TKey, TValue>(
          keySerializer.Deserialize(i.Key),
          valueSerializer.Deserialize(i.Value)));

    byte[] Factory(in byte[] key)
    {
      return valueSerializer.Serialize(valueFactory(keySerializer.Deserialize(key)));
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange<TArg>(
    IEnumerable<TKey> keys,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.ValueFactoryWithKeyAndArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    return BaseDB
      .GetOrAddRange(
        keys.Select(i => keySerializer.Serialize(i)),
        Factory,
        factoryArgument)
      .Select(
        i => new KeyValuePair<TKey, TValue>(
          keySerializer.Deserialize(i.Key),
          valueSerializer.Deserialize(i.Value)));

    byte[] Factory(in byte[] key, TArg arg)
    {
      return valueSerializer.Serialize(valueFactory(keySerializer.Deserialize(key), arg));
    }
  }

  /// <inheritdoc />
  public IEnumerable<TKey> KeysIterator =>
    BaseDB.KeysIterator.Select(k => keySerializer.Deserialize(k));

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void SetValue(TKey key, TValue value)
  {
    BaseDB.SetValue(keySerializer.Serialize(key), valueSerializer.Serialize(value));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryAdd(TKey key, TValue value)
  {
    return BaseDB.TryAdd(keySerializer.Serialize(key), valueSerializer.Serialize(value));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryAdd(TKey key, TValue value, out TValue existing)
  {
    if (!BaseDB.TryAdd(
          keySerializer.Serialize(key),
          valueSerializer.Serialize(value),
          out var raw)) {
      existing = valueSerializer.Deserialize(raw);

      return false;
    }

    existing = default!;

    return true;
  }

  /// <inheritdoc />
  public (long, long) TryAdd(IEnumerable<KeyValuePair<TKey, TValue>> pairs)
  {
    return pairs is TypedPlaneDB<TKey, TValue> bdb &&
           bdb.keySerializer.GetType() == keySerializer.GetType() &&
           bdb.valueSerializer.GetType() == valueSerializer.GetType()
      ? BaseDB.TryAdd(bdb.BaseDB)
      : BaseDB.TryAdd(Generate());

    IEnumerable<KeyValuePair<byte[], byte[]>> Generate()
    {
      foreach (var kv in pairs) {
        yield return new KeyValuePair<byte[], byte[]>(
          keySerializer.Serialize(kv.Key),
          valueSerializer.Serialize(kv.Value));
      }
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryRemove(TKey key, [MaybeNullWhen(false)] out TValue value)
  {
    if (BaseDB.TryRemove(keySerializer.Serialize(key), out var raw)) {
      value = valueSerializer.Deserialize(raw);

      return true;
    }

    value = default!;

    return false;
  }

  /// <inheritdoc />
  public long TryRemove(IEnumerable<TKey> keys)
  {
    return BaseDB.TryRemove(keys.Select(k => keySerializer.Serialize(k)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
  {
    return BaseDB.TryUpdate(
      keySerializer.Serialize(key),
      valueSerializer.Serialize(newValue),
      valueSerializer.Serialize(comparisonValue));
  }

  /// <inheritdoc />
  public bool TryUpdate(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.TryUpdateFactory updateFactory)
  {
    return BaseDB.TryUpdate(
      keySerializer.Serialize(key),
      (in byte[] _, in byte[] existing, [MaybeNullWhen(false)] out byte[] newValue) => {
        if (!updateFactory(key, valueSerializer.Deserialize(existing), out var value)) {
          newValue = null!;

          return false;
        }

        newValue = valueSerializer.Serialize(value);

        return true;
      });
  }

  /// <inheritdoc />
  public bool TryUpdate<TArg>(
    TKey key,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.TryUpdateFactoryWithArg<TArg> updateFactory,
    TArg arg)
  {
    return BaseDB.TryUpdate(
      keySerializer.Serialize(key),
      (in byte[] existing, TArg _, [MaybeNullWhen(false)] out byte[] newValue) => {
        if (!updateFactory(valueSerializer.Deserialize(existing), arg, out var value)) {
          newValue = null!;

          return false;
        }

        newValue = valueSerializer.Serialize(value);

        return true;
      },
      arg);
  }

  /// <summary>
  ///   Dispose this DB
  /// </summary>
  /// <param name="disposing">Disposing (or finalizing)</param>
  protected virtual void Dispose(bool disposing)
  {
    if (disposing) {
      BaseDB.Dispose();
    }
  }
}

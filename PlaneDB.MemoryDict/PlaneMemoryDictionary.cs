using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc cref="IPlaneDictionary{TKey,TValue}" />
/// <summary>
///   An in-memory dictionary
/// </summary>
/// <remarks>
///   Unlike PlaneDB types, this memory dictionary will not make (implicit) copies of data. Modifying keys and value
///   after adding them is therefore undefined behavior.
/// </remarks>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
[PublicAPI]
public sealed class
  PlaneMemoryDictionary<TKey, TValue> : ConcurrentDictionary<TKey, TValue>,
  IPlaneDictionary<TKey, TValue> where TKey : notnull
{
  /// <summary>
  ///   Construct a new instance with the default comparer
  /// </summary>
  public PlaneMemoryDictionary()
  {
  }

  /// <summary>
  ///   Construct a new instance with an initial collection
  /// </summary>
  /// <param name="items">Initial collection</param>
  public PlaneMemoryDictionary(IEnumerable<KeyValuePair<TKey, TValue>> items) : base(
    items)
  {
  }

  /// <summary>
  ///   Construct a new instance with a specialized comparer
  /// </summary>
  /// <param name="comparer"></param>
  public PlaneMemoryDictionary(IEqualityComparer<TKey> comparer) : base(comparer)
  {
  }

  /// <summary>
  ///   Construct a new instance with an initial collection and a specialized comparer
  /// </summary>
  /// <param name="items">Initial collection</param>
  /// <param name="comparer"></param>
  public PlaneMemoryDictionary(
    IEnumerable<KeyValuePair<TKey, TValue>> items,
    IEqualityComparer<TKey> comparer) : base(items, comparer)
  {
  }

  /// <inheritdoc />
  public async IAsyncEnumerator<KeyValuePair<TKey, TValue>> GetAsyncEnumerator(
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
  public bool Remove(KeyValuePair<TKey, TValue> item)
  {
    return TryRemove(item.Key, out _);
  }

  /// <inheritdoc />
  public void Dispose()
  {
    Clear();
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public TValue AddOrUpdate<TArg>(
    TKey key,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.ValueFactoryWithArg<TArg> addValueFactory,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.UpdateValueFactoryWithArg<TArg> updateValueFactory,
    TArg factoryArgument)
  {
    ConcurrentDictionary<TKey, TValue> self = this;

    return self.AddOrUpdate(
      key,
      _ => addValueFactory(factoryArgument),
      (_, value) => updateValueFactory(value, factoryArgument));
  }

  /// <inheritdoc />
  public TValue AddOrUpdate(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactory addValueFactory,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.UpdateValueFactory updateValueFactory)
  {
    ConcurrentDictionary<TKey, TValue> self = this;

    return self.AddOrUpdate(
      key,
      _ => addValueFactory(),
      (_, value) => updateValueFactory(value));
  }

  /// <inheritdoc />
  public TValue AddOrUpdate(
    TKey key,
    TValue addValue,
    IPlaneDictionary<TKey, TValue>.UpdateValueFactory updateValueFactory)
  {
    ConcurrentDictionary<TKey, TValue> self = this;

    return self.AddOrUpdate(key, addValue, (_, value) => updateValueFactory(value));
  }

  /// <inheritdoc />
  public void CopyTo(IDictionary<TKey, TValue> destination)
  {
    foreach (var (key, value) in this) {
      destination.Add(key, value);
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<TKey> GetKeysIteratorAsync(
    [EnumeratorCancellation] CancellationToken token)
  {
    await Task.Yield();
    foreach (var key in Keys) {
      token.ThrowIfCancellationRequested();

      yield return key;
    }
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public TValue GetOrAdd(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactory valueFactory)
  {
    ConcurrentDictionary<TKey, TValue> self = this;

    return self.GetOrAdd(key, _ => valueFactory());
  }

  /// <inheritdoc />
  public TValue GetOrAdd(TKey key, TValue value, out bool added)
  {
    ConcurrentDictionary<TKey, TValue> self = this;
    var innerAdded = false;
    var rv = self.GetOrAdd(
      key,
      _ => {
        innerAdded = true;

        return value;
      });
    added = innerAdded;

    return rv;
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public TValue GetOrAdd<TArg>(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactoryWithArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    ConcurrentDictionary<TKey, TValue> self = this;

    return self.GetOrAdd(key, _ => valueFactory(factoryArgument));
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<KeyValuePair<TKey, TValue>> keysAndDefaults)
  {
    return keysAndDefaults.Select(
      kv => new KeyValuePair<TKey, TValue>(kv.Key, GetOrAdd(kv.Key, kv.Value)));
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    TValue value)
  {
    return keys.Select(key => new KeyValuePair<TKey, TValue>(key, GetOrAdd(key, value)));
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange(
    [InstantHandle] IEnumerable<TKey> keys,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.ValueFactoryWithKey valueFactory)
  {
    return keys.Select(
      key => new KeyValuePair<TKey, TValue>(key, GetOrAdd(key, () => valueFactory(key))));
  }

  /// <inheritdoc />
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public IEnumerable<KeyValuePair<TKey, TValue>> GetOrAddRange<TArg>(
    [InstantHandle] IEnumerable<TKey> keys,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.ValueFactoryWithKeyAndArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    return keys.Select(
      key => new KeyValuePair<TKey, TValue>(
        key,
        GetOrAdd(key, arg => valueFactory(key, arg), factoryArgument)));
  }

  /// <inheritdoc />
  public IEnumerable<TKey> KeysIterator => Keys;

  /// <inheritdoc />
  public void SetValue(TKey key, TValue value)
  {
    ConcurrentDictionary<TKey, TValue> self = this;
    self[key] = value;
  }

  /// <inheritdoc />
  public bool TryAdd(TKey key, TValue value, out TValue existing)
  {
    ConcurrentDictionary<TKey, TValue> self = this;
    TValue innerExisting = default!;
    var innerAdded = true;
    _ = self.AddOrUpdate(
      key,
      value,
      (_, e) => {
        innerExisting = e;
        innerAdded = false;

        return e;
      });
    existing = innerExisting;

    return innerAdded;
  }

  /// <inheritdoc />
  public (long, long) TryAdd(IEnumerable<KeyValuePair<TKey, TValue>> pairs)
  {
    var added = 0L;
    var existing = 0L;
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
  public long TryRemove(IEnumerable<TKey> keys)
  {
    return keys.Sum(k => TryRemove(k, out _) ? 1 : 0);
  }

  /// <inheritdoc />
  public bool TryUpdate(
    TKey key,
    [InstantHandle] IPlaneDictionary<TKey, TValue>.TryUpdateFactory updateFactory)
  {
    try {
      _ = AddOrUpdate(
        key,
        _ => throw new KeyNotFoundException(),
        (_, existing) => !updateFactory(key, existing, out var newValue)
          ? throw new KeyNotFoundException()
          : newValue);

      return true;
    }
    catch (KeyNotFoundException) {
      return false;
    }
  }

  /// <inheritdoc />
  public bool TryUpdate<TArg>(
    TKey key,
    [InstantHandle]
    IPlaneDictionary<TKey, TValue>.TryUpdateFactoryWithArg<TArg> updateFactory,
    TArg arg)
  {
    try {
      _ = AddOrUpdate(
        key,
        _ => throw new KeyNotFoundException(),
        (_, existing) => !updateFactory(existing, arg, out var newValue)
          ? throw new KeyNotFoundException()
          : newValue);

      return true;
    }
    catch (KeyNotFoundException) {
      return false;
    }
  }
}

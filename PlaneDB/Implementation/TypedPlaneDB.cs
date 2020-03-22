using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using NMaier.Serializers;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>
  ///   A simple Key-Value store
  /// </summary>
  [PublicAPI]
  [SuppressMessage("ReSharper", "UseDeconstruction")]
  [SuppressMessage("ReSharper", "UseDeconstructionOnParameter")]
  public class TypedPlaneDB<TKey, TValue> : IPlaneDB<TKey, TValue>
  {
    private readonly ISerializer<TKey> keySerializer;
    private readonly ISerializer<TValue> valueSerializer;
    private readonly PlaneDB wrapped;

    /// <summary>
    ///   Create a new typed Key-Value store
    /// </summary>
    /// <remarks>
    ///   Please note that the internal sort order will still be based upon the byte-array comparer
    /// </remarks>
    /// <param name="keySerializer">Serializer to use to handle keys</param>
    /// <param name="valueSerializer">Serializer to use to handle values</param>
    /// <param name="location">Directory that will store the PlaneDB</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    public TypedPlaneDB(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer, DirectoryInfo location,
      FileMode mode, PlaneDBOptions options)
    {
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      wrapped = new PlaneDB(location, mode, options);
      wrapped.OnFlushMemoryTable += (sender, db) => OnFlushMemoryTable?.Invoke(this, this);
      wrapped.OnMergedTables += (sender, db) => OnMergedTables?.Invoke(this, this);
    }

    /// <inheritdoc />
    public void Add(KeyValuePair<TKey, TValue> item)
    {
      wrapped.Add(keySerializer.Serialize(item.Key), valueSerializer.Serialize(item.Value));
    }

    /// <inheritdoc />
    public void Clear()
    {
      wrapped.Clear();
    }

    /// <inheritdoc />
    public bool Contains(KeyValuePair<TKey, TValue> item)
    {
      return ContainsKey(item.Key);
    }

    /// <inheritdoc />
    public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
    {
      this.ToArray().CopyTo(array, arrayIndex);
    }

    /// <inheritdoc />
    public int Count => wrapped.Count;

    /// <inheritdoc />
    public bool IsReadOnly => wrapped.IsReadOnly;

    /// <inheritdoc />
    public bool Remove(KeyValuePair<TKey, TValue> item)
    {
      return wrapped.Remove(keySerializer.Serialize(item.Key));
    }

    /// <inheritdoc />
    public void Add(TKey key, TValue value)
    {
      wrapped.Add(keySerializer.Serialize(key), valueSerializer.Serialize(value));
    }

    /// <inheritdoc />
    public bool ContainsKey(TKey key)
    {
      return wrapped.ContainsKey(keySerializer.Serialize(key));
    }

    /// <inheritdoc />
    public TValue this[TKey key]
    {
      get => valueSerializer.Deserialize(wrapped[keySerializer.Serialize(key)]);
      set => wrapped.Set(keySerializer.Serialize(key), valueSerializer.Serialize(value));
    }

    /// <inheritdoc />
    public ICollection<TKey> Keys => wrapped.Keys.Select(k => keySerializer.Deserialize(k)).ToArray();

    /// <inheritdoc />
    public bool Remove(TKey key)
    {
      return wrapped.Remove(keySerializer.Serialize(key));
    }

#pragma warning disable CS8614 // Nullability of reference types in type of parameter doesn't match implicitly implemented member.
    /// <inheritdoc />
    public bool TryGetValue(TKey key, out TValue value)
#pragma warning restore CS8614 // Nullability of reference types in type of parameter doesn't match implicitly implemented member.
    {
      if (wrapped.TryGetValue(keySerializer.Serialize(key), out var raw)) {
        value = valueSerializer.Deserialize(raw);
        return true;
      }

#pragma warning disable CS8601 // Possible null reference assignment.
      value = default;
#pragma warning restore CS8601 // Possible null reference assignment.
      return false;
    }

    /// <inheritdoc />
    public ICollection<TValue> Values => wrapped.Values.Select(v => valueSerializer.Deserialize(v)).ToArray();

    /// <inheritdoc />
    public void Dispose()
    {
      Dispose(true);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return ((IEnumerable)wrapped).GetEnumerator();
    }

    /// <inheritdoc />
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
      IEnumerable<KeyValuePair<TKey, TValue>> Stream()
      {
        foreach (var kv in wrapped) {
          yield return new KeyValuePair<TKey, TValue>(keySerializer.Deserialize(kv.Key),
                                                      valueSerializer.Deserialize(kv.Value));
        }
      }

      return Stream().GetEnumerator();
    }

    /// <inheritdoc />
    public void Compact()
    {
      wrapped.Compact();
    }

    /// <inheritdoc />
    public long CurrentBloomBits => wrapped.CurrentBloomBits;

    /// <inheritdoc />
    public long CurrentDiskSize => wrapped.CurrentDiskSize;

    /// <inheritdoc />
    public long CurrentIndexBlockCount => wrapped.CurrentIndexBlockCount;

    /// <inheritdoc />
    public long CurrentRealSize => wrapped.CurrentRealSize;

    /// <inheritdoc />
    public int CurrentTableCount => wrapped.CurrentTableCount;

    /// <inheritdoc />
    public void Flush()
    {
      wrapped.Flush();
    }

    /// <inheritdoc />
    public DirectoryInfo Location => wrapped.Location;

    /// <inheritdoc />
    public void MassInsert(Action action)
    {
      wrapped.MassInsert(action);
    }

    /// <inheritdoc />
    public string TableSpace => wrapped.TableSpace;

    /// <inheritdoc />
    [SuppressMessage("ReSharper", "ImplicitlyCapturedClosure")]
    public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory,
      Func<TKey, TValue, TValue> updateValueFactory)
    {
      byte[] AddValueFactory(byte[] _)
      {
        return valueSerializer.Serialize(addValueFactory(key));
      }

      byte[] UpdateValueFactory(byte[] _, byte[] raw)
      {
        return valueSerializer.Serialize(updateValueFactory(key, valueSerializer.Deserialize(raw)));
      }

      return valueSerializer.Deserialize(
        wrapped.AddOrUpdate(keySerializer.Serialize(key), AddValueFactory, UpdateValueFactory));
    }

    /// <inheritdoc />
    public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
    {
      byte[] UpdateValueFactory(byte[] _, byte[] raw)
      {
        return valueSerializer.Serialize(updateValueFactory(key, valueSerializer.Deserialize(raw)));
      }

      return valueSerializer.Deserialize(wrapped.AddOrUpdate(keySerializer.Serialize(key),
                                                             valueSerializer.Serialize(addValue), UpdateValueFactory));
    }

    /// <inheritdoc />
    [SuppressMessage("ReSharper", "ImplicitlyCapturedClosure")]
    public TValue AddOrUpdate<TArg>(TKey key, Func<TKey, TArg, TValue> addValueFactory,
      Func<TKey, TValue, TArg, TValue> updateValueFactory, TArg factoryArgument)
    {
      byte[] AddValueFactory(byte[] _, TArg arg)
      {
        return valueSerializer.Serialize(addValueFactory(key, arg));
      }

      byte[] UpdateValueFactory(byte[] _, byte[] raw, TArg arg)
      {
        return valueSerializer.Serialize(updateValueFactory(key, valueSerializer.Deserialize(raw), arg));
      }

      return valueSerializer.Deserialize(
        wrapped.AddOrUpdate(keySerializer.Serialize(key), AddValueFactory, UpdateValueFactory, factoryArgument));
    }

    /// <inheritdoc />
    public void CopyTo(IDictionary<TKey, TValue> destination)
    {
      switch (destination) {
        case TypedPlaneDB<TKey, TValue> pother:
          pother.wrapped.CopyTo(wrapped);
          return;
        case IPlaneDB<TKey, TValue> other:
          other.MassInsert(() => {
            foreach (var kv in this) {
              other.Set(kv.Key, kv.Value);
            }
          });
          return;
        default:
          foreach (var kv in this) {
            destination[kv.Key] = kv.Value;
          }

          return;
      }
    }

    /// <inheritdoc />
    public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
    {
      byte[] Factory(byte[] _)
      {
        return valueSerializer.Serialize(valueFactory(key));
      }

      return valueSerializer.Deserialize(wrapped.GetOrAdd(keySerializer.Serialize(key), Factory));
    }

    /// <inheritdoc />
    public TValue GetOrAdd(TKey key, TValue value)
    {
      return valueSerializer.Deserialize(wrapped.GetOrAdd(keySerializer.Serialize(key),
                                                          valueSerializer.Serialize(value)));
    }

    /// <inheritdoc />
    public TValue GetOrAdd(TKey key, TValue value, out bool added)
    {
      return valueSerializer.Deserialize(wrapped.GetOrAdd(keySerializer.Serialize(key),
                                                          valueSerializer.Serialize(value), out added));
    }

    /// <inheritdoc />
    public TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> valueFactory, TArg factoryArgument)
    {
      byte[] Factory(byte[] _, TArg arg)
      {
        return valueSerializer.Serialize(valueFactory(key, arg));
      }

      return valueSerializer.Deserialize(wrapped.GetOrAdd(keySerializer.Serialize(key), Factory, factoryArgument));
    }

    /// <inheritdoc />
    public IEnumerable<TKey> KeysIterator => wrapped.KeysIterator.Select(k => keySerializer.Deserialize(k));

    // XXX
    /// <inheritdoc />
    public event EventHandler<IPlaneDB<TKey, TValue>>? OnFlushMemoryTable;

    // XXX
    /// <inheritdoc />
    public event EventHandler<IPlaneDB<TKey, TValue>>? OnMergedTables;

    /// <inheritdoc />
    public void Set(TKey key, TValue value)
    {
      wrapped.Set(keySerializer.Serialize(key), valueSerializer.Serialize(value));
    }


    /// <inheritdoc />
    public bool TryAdd(TKey key, TValue value)
    {
      return wrapped.TryAdd(keySerializer.Serialize(key), valueSerializer.Serialize(value));
    }

    /// <inheritdoc />
    public bool TryAdd(TKey key, TValue value, out TValue existing)
    {
      if (!wrapped.TryAdd(keySerializer.Serialize(key), valueSerializer.Serialize(value), out var raw)) {
        existing = valueSerializer.Deserialize(raw);
        return false;
      }

#pragma warning disable CS8601 // Possible null reference assignment.
      existing = default;
#pragma warning restore CS8601 // Possible null reference assignment.
      return true;
    }

    /// <inheritdoc />
    public bool TryRemove(TKey key, out TValue value)
    {
      if (wrapped.TryRemove(keySerializer.Serialize(key), out var raw)) {
        value = valueSerializer.Deserialize(raw);
        return true;
      }

#pragma warning disable CS8601 // Possible null reference assignment.
      value = default;
#pragma warning restore CS8601 // Possible null reference assignment.
      return false;
    }

    /// <inheritdoc />
    public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
    {
      return wrapped.TryUpdate(keySerializer.Serialize(key), valueSerializer.Serialize(newValue),
                               valueSerializer.Serialize(comparisonValue));
    }

    /// <summary>
    ///   Dispose this DB
    /// </summary>
    /// <param name="disposing">Disposing (or finalizing)</param>
    protected virtual void Dispose(bool disposing)
    {
      if (disposing) {
        wrapped?.Dispose();
      }

      GC.SuppressFinalize(this);
    }
  }
}
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using NMaier.Serializers;

namespace NMaier.PlaneDB
{
  /// <inheritdoc />
  /// <summary>Your typed persistent set</summary>
  [PublicAPI]
  public class TypedPlaneSet<T> : IPlaneSet<T>
  {
    private readonly ISerializer<T> serializer;
    private PlaneSet wrapped;

    /// <inheritdoc />
    /// <summary>
    ///   Creates a new typed persistent set
    /// </summary>
    /// <param name="serializer">Serializer to use</param>
    /// <param name="location">Directory that will store the PlaneSet</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    public TypedPlaneSet(ISerializer<T> serializer, DirectoryInfo location, FileMode mode, PlaneDBOptions options)
    {
      this.serializer = serializer;
      wrapped = new PlaneSet(location, mode, options);
    }

    /// <inheritdoc />
    public void CopyTo(Array array, int index)
    {
      ToArray().CopyTo(array, index);
    }

    /// <inheritdoc />
    public bool IsSynchronized => wrapped.IsSynchronized;

    /// <inheritdoc />
    public object SyncRoot => wrapped.SyncRoot;

    void ICollection<T>.Add(T item)
    {
      ((ICollection<byte[]>)wrapped).Add(serializer.Serialize(item));
    }

    /// <inheritdoc />
    public void Clear()
    {
      wrapped.Clear();
    }

    /// <inheritdoc />
    public bool Contains(T item)
    {
      return wrapped.Contains(serializer.Serialize(item));
    }

    void ICollection<T>.CopyTo(T[] array, int arrayIndex)
    {
      wrapped.CopyTo(array, arrayIndex);
    }

    /// <inheritdoc />
    public bool IsReadOnly => wrapped.IsReadOnly;

    /// <inheritdoc />
    public bool Remove(T item)
    {
      return wrapped.Remove(serializer.Serialize(item));
    }

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
    public IEnumerator<T> GetEnumerator()
    {
      IEnumerable<T> Stream()
      {
        foreach (var kv in wrapped) {
          yield return serializer.Deserialize(kv);
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

    void IProducerConsumerCollection<T>.CopyTo(T[] array, int index)
    {
      ToArray().CopyTo(array, index);
    }

    /// <inheritdoc />
    public T[] ToArray()
    {
      return wrapped.Select(i => serializer.Deserialize(i)).ToArray();
    }

    /// <inheritdoc />
    public bool TryAdd(T item)
    {
      return wrapped.TryAdd(serializer.Serialize(item));
    }

    /// <inheritdoc />
    public bool TryTake(out T item)
    {
      if (wrapped.TryTake(out var i)) {
        item = serializer.Deserialize(i);
        return true;
      }

#pragma warning disable CS8601 // Possible null reference assignment.
      item = default;
#pragma warning restore CS8601 // Possible null reference assignment.
      return false;
    }

    /// <inheritdoc cref="ICollection.Count" />
    public int Count => wrapped.Count;

    bool ISet<T>.Add(T item)
    {
      return ((ISet<byte[]>)wrapped).Add(serializer.Serialize(item));
    }

    /// <inheritdoc />
    public void ExceptWith(IEnumerable<T> other)
    {
      wrapped.ExceptWith(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public void IntersectWith(IEnumerable<T> other)
    {
      wrapped.IntersectWith(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool IsProperSubsetOf(IEnumerable<T> other)
    {
      return wrapped.IsProperSubsetOf(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool IsProperSupersetOf(IEnumerable<T> other)
    {
      return wrapped.IsProperSupersetOf(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool IsSubsetOf(IEnumerable<T> other)
    {
      return wrapped.IsSubsetOf(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool IsSupersetOf(IEnumerable<T> other)
    {
      return wrapped.IsSupersetOf(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool Overlaps(IEnumerable<T> other)
    {
      return wrapped.Overlaps(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public bool SetEquals(IEnumerable<T> other)
    {
      return wrapped.SetEquals(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public void SymmetricExceptWith(IEnumerable<T> other)
    {
      wrapped.SymmetricExceptWith(other.Select(item => serializer.Serialize(item)));
    }

    /// <inheritdoc />
    public void UnionWith(IEnumerable<T> other)
    {
      wrapped.UnionWith(other.Select(item => serializer.Serialize(item)));
    }

    /// <summary>
    ///   Dispose this instance
    /// </summary>
    /// <param name="disposing">Disposing (or finalizing)</param>
    protected virtual void Dispose(bool disposing)
    {
      if (disposing) {
        wrapped.Dispose();
      }
    }
  }
}
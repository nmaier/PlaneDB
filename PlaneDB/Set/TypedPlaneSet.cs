using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>Your typed persistent set</summary>
[PublicAPI]
public class TypedPlaneSet<T> : IPlaneSet<T>
{
  private readonly
    Dictionary<IPlaneSetMergeParticipant<T>, IPlaneDBMergeParticipant<byte[], byte[]>>
    participants = [];

  private readonly IPlaneSerializer<T> serializer;
  private readonly PlaneSet wrappedSet;

  /// <summary>
  ///   Creates a new typed persistent set
  /// </summary>
  /// <param name="serializer">Serializer to use</param>
  /// <param name="location">Directory that will store the PlaneSet</param>
  /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
  public TypedPlaneSet(
    IPlaneSerializer<T> serializer,
    DirectoryInfo location,
    PlaneOptions options)
  {
    this.serializer = serializer;
    wrappedSet = new PlaneSet(location, options);
  }

  internal TypedPlaneSet(IPlaneSerializer<T> serializer, IPlaneDB<byte[], byte[]> baseDB)
  {
    this.serializer = serializer;
    wrappedSet = new PlaneSet(baseDB);
  }

  /// <inheritdoc />
  public void CopyTo(Array array, int index)
  {
    foreach (var key in BaseDB.KeysIterator) {
      array.SetValue(serializer.Deserialize(key), index++);
    }
  }

  /// <inheritdoc />
  public bool IsSynchronized => wrappedSet.IsSynchronized;

  /// <inheritdoc />
  public object SyncRoot => wrappedSet.SyncRoot;

  [MethodImpl(Constants.SHORT_METHOD)]
  void ICollection<T>.Add(T item)
  {
    ((ICollection<byte[]>)wrappedSet).Add(serializer.Serialize(item));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Clear()
  {
    wrappedSet.Clear();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Contains(T item)
  {
    return wrappedSet.Contains(serializer.Serialize(item));
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  void ICollection<T>.CopyTo(T[] array, int arrayIndex)
  {
    foreach (var key in BaseDB.KeysIterator) {
      array[arrayIndex++] = serializer.Deserialize(key);
    }
  }

  /// <inheritdoc />
  public bool IsReadOnly => wrappedSet.IsReadOnly;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(T item)
  {
    return wrappedSet.Remove(serializer.Serialize(item));
  }

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
    return GetEnumerator();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  [MustDisposeResource]
  public IEnumerator<T> GetEnumerator()
  {
    return Stream().GetEnumerator();

    IEnumerable<T> Stream()
    {
      foreach (var kv in wrappedSet) {
        yield return serializer.Deserialize(kv);
      }
    }
  }

  /// <inheritdoc />
  public IPlaneDB<byte[], byte[]> BaseDB => wrappedSet.BaseDB;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Compact(CompactionMode mode = CompactionMode.Normal)
  {
    wrappedSet.Compact(mode);
  }

  /// <inheritdoc />
  public long CurrentBloomBits => wrappedSet.CurrentBloomBits;

  /// <inheritdoc />
  public long CurrentDiskSize => wrappedSet.CurrentDiskSize;

  /// <inheritdoc />
  public long CurrentIndexBlockCount => wrappedSet.CurrentIndexBlockCount;

  /// <inheritdoc />
  public long CurrentRealSize => wrappedSet.CurrentRealSize;

  /// <inheritdoc />
  public int CurrentTableCount => wrappedSet.CurrentTableCount;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Flush()
  {
    wrappedSet.Flush();
  }

  /// <inheritdoc />
  public DirectoryInfo Location => wrappedSet.Location;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void MassInsert([InstantHandle] Action action)
  {
    wrappedSet.MassInsert(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TResult MassInsert<TResult>(Func<TResult> action)
  {
    return wrappedSet.MassInsert(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void MassRead(Action action)
  {
    wrappedSet.MassRead(action);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public TResult MassRead<TResult>(Func<TResult> action)
  {
    return wrappedSet.MassRead(action);
  }

  /// <inheritdoc />
  public PlaneOptions Options => wrappedSet.Options;

  /// <inheritdoc />
  public string TableSpace => wrappedSet.TableSpace;

  /// <inheritdoc />
  public void RegisterMergeParticipant(IPlaneSetMergeParticipant<T> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.RegisterMergeParticipant(old);
      }

      var wrapped = new SetParticipantWrapper<T>(serializer, participant);
      participants.Add(participant, wrapped);
      BaseDB.RegisterMergeParticipant(wrapped);
    }
  }

  /// <inheritdoc />
  public void UnregisterMergeParticipant(IPlaneSetMergeParticipant<T> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.RegisterMergeParticipant(old);
      }
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  void IProducerConsumerCollection<T>.CopyTo(T[] array, int index)
  {
    foreach (var key in BaseDB.KeysIterator) {
      array[index++] = serializer.Deserialize(key);
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public T[] ToArray()
  {
    return wrappedSet.Select(i => serializer.Deserialize(i)).ToArray();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryAdd(T item)
  {
    return wrappedSet.TryAdd(serializer.Serialize(item));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryTake(out T item)
  {
    if (wrappedSet.TryTake(out var i)) {
      item = serializer.Deserialize(i);

      return true;
    }

    item = default!;

    return false;
  }

  /// <inheritdoc cref="ICollection.Count" />
  public int Count => wrappedSet.Count;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  bool ISet<T>.Add(T item)
  {
    return ((ISet<byte[]>)wrappedSet).Add(serializer.Serialize(item));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void ExceptWith(IEnumerable<T> other)
  {
    wrappedSet.ExceptWith(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void IntersectWith(IEnumerable<T> other)
  {
    wrappedSet.IntersectWith(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsProperSubsetOf(IEnumerable<T> other)
  {
    return wrappedSet.IsProperSubsetOf(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsProperSupersetOf(IEnumerable<T> other)
  {
    return wrappedSet.IsProperSupersetOf(
      other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  public bool IsSubsetOf(IEnumerable<T> other)
  {
    return wrappedSet.IsSubsetOf(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsSupersetOf(IEnumerable<T> other)
  {
    return wrappedSet.IsSupersetOf(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Overlaps(IEnumerable<T> other)
  {
    return wrappedSet.Overlaps(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool SetEquals(IEnumerable<T> other)
  {
    return wrappedSet.SetEquals(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void SymmetricExceptWith(IEnumerable<T> other)
  {
    wrappedSet.SymmetricExceptWith(other.Select(item => serializer.Serialize(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void UnionWith(IEnumerable<T> other)
  {
    wrappedSet.UnionWith(other.Select(item => serializer.Serialize(item)));
  }

  /// <summary>
  ///   Dispose this instance
  /// </summary>
  /// <param name="disposing">Disposing (or finalizing)</param>
  protected virtual void Dispose(bool disposing)
  {
    if (disposing) {
      wrappedSet.Dispose();
    }
  }
}

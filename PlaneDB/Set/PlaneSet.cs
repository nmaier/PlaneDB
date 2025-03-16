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
/// <summary>Your byte[] persistent set</summary>
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
public sealed class PlaneSet : IPlaneSet<byte[]>
{
  private readonly
    Dictionary<IPlaneSetMergeParticipant<byte[]>,
      IPlaneDBMergeParticipant<byte[], byte[]>> participants = [];

  /// <param name="location">Directory that will store the PlaneSet</param>
  /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
  /// <summary>Opens or creates a new PlaneSet</summary>
  [CollectionAccess(CollectionAccessType.UpdatedContent)]
  public PlaneSet(DirectoryInfo location, PlaneOptions options)
  {
    BaseDB = new PlaneDB(location, options);
  }

  internal PlaneSet(IPlaneDB<byte[], byte[]> baseDB)
  {
    BaseDB = baseDB;
  }

  /// <inheritdoc />
  public void CopyTo(Array array, int index)
  {
    foreach (var key in BaseDB.KeysIterator) {
      array.SetValue(key, index++);
    }
  }

  /// <inheritdoc />
  public bool IsSynchronized => false;

  /// <inheritdoc />
  public object SyncRoot { get; } = new();

  [MethodImpl(Constants.SHORT_METHOD)]
  void ICollection<byte[]>.Add(byte[] item)
  {
    BaseDB.Add(item ?? throw new ArgumentNullException(nameof(item)), []);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Clear()
  {
    BaseDB.Clear();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Contains(byte[] item)
  {
    return BaseDB.ContainsKey(item ?? throw new ArgumentNullException(nameof(item)));
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  void ICollection<byte[]>.CopyTo(byte[][] array, int arrayIndex)
  {
    foreach (var key in BaseDB.KeysIterator) {
      array[arrayIndex++] = key;
    }
  }

  /// <inheritdoc />
  public bool IsReadOnly => Options.ReadOnly;

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Remove(byte[] item)
  {
    return BaseDB.TryRemove(item, out _);
  }

  /// <inheritdoc />
  public void Dispose()
  {
    BaseDB.Dispose();
  }

  [MustDisposeResource]
  IEnumerator IEnumerable.GetEnumerator()
  {
    return BaseDB.KeysIterator.GetEnumerator();
  }

  /// <inheritdoc />
  [MustDisposeResource]
  public IEnumerator<byte[]> GetEnumerator()
  {
    return BaseDB.KeysIterator.GetEnumerator();
  }

  /// <inheritdoc />
  public IPlaneDB<byte[], byte[]> BaseDB { get; }

  /// <inheritdoc />
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
  public void RegisterMergeParticipant(IPlaneSetMergeParticipant<byte[]> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.RegisterMergeParticipant(old);
      }

      var wrapped = new SetParticipantWrapper<byte[]>(
        new PlanePassthroughSerializer(),
        participant);
      participants.Add(participant, wrapped);
      BaseDB.RegisterMergeParticipant(wrapped);
    }
  }

  /// <inheritdoc />
  public void UnregisterMergeParticipant(IPlaneSetMergeParticipant<byte[]> participant)
  {
    lock (participants) {
      if (participants.Remove(participant, out var old)) {
        BaseDB.RegisterMergeParticipant(old);
      }
    }
  }

  void IProducerConsumerCollection<byte[]>.CopyTo(byte[][] array, int index)
  {
    foreach (var k in BaseDB.KeysIterator) {
      array[index++] = k;
    }
  }

  /// <inheritdoc />
  public byte[][] ToArray()
  {
    return BaseDB.KeysIterator.ToArray();
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryAdd(byte[] item)
  {
    return BaseDB.TryAdd(item, []);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool TryTake(out byte[] item)
  {
    var enumerator = BaseDB.KeysIterator.GetEnumerator();
    try {
      while (enumerator.MoveNext()) {
        var current = enumerator.Current;
        if (!BaseDB.TryRemove(current, out _)) {
          continue;
        }

        item = current;

        return true;
      }

      item = [];

      return false;
    }
    finally {
      enumerator.Dispose();
    }
  }

  /// <inheritdoc cref="ICollection.Count" />
  public int Count => BaseDB.Count;

  [MethodImpl(Constants.SHORT_METHOD)]
  bool ISet<byte[]>.Add(byte[] item)
  {
    return BaseDB.TryAdd(item, []);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void ExceptWith(IEnumerable<byte[]> other)
  {
    BaseDB.MassInsert(
      () => {
        foreach (var item in other) {
          _ = BaseDB.TryRemove(item, out _);
        }
      });
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void IntersectWith(IEnumerable<byte[]> other)
  {
    var cmp = PlaneByteArrayComparer.Default;
    var hs = new HashSet<byte[]>(other.Distinct(cmp), cmp);
    foreach (var item in this) {
      if (!hs.Contains(item)) {
        _ = Remove(item);
      }
    }
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsProperSubsetOf(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(
      () => {
        var count = 0;
        var counter = 0;
        foreach (var item in other) {
          count++;
          if (Contains(item)) {
            counter++;
          }
        }

        return counter != count && counter == Count;
      });
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsProperSupersetOf(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(
      () => {
        var count = 0;
        foreach (var item in other) {
          if (!Contains(item)) {
            return false;
          }

          count++;
        }

        return count != Count;
      });
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsSubsetOf(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(() => other.Count(Contains) == Count);
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsSupersetOf(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(() => other.All(Contains));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool Overlaps(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(() => other.Any(item => BaseDB.ContainsKey(item)));
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public bool SetEquals(IEnumerable<byte[]> other)
  {
    return BaseDB.MassRead(
      () => {
        var counted = 0L;
        foreach (var item in other) {
          if (!BaseDB.ContainsKey(item)) {
            return false;
          }

          ++counted;
        }

        return counted == Count;
      });
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void SymmetricExceptWith(IEnumerable<byte[]> other)
  {
    BaseDB.MassInsert(
      () => {
        foreach (var item in other) {
          if (TryAdd(item)) {
            continue;
          }

          _ = Remove(item);
        }
      });
  }

  /// <inheritdoc />
  [MethodImpl(Constants.SHORT_METHOD)]
  public void UnionWith(IEnumerable<byte[]> other)
  {
    BaseDB.MassInsert(
      () => {
        foreach (var item in other) {
          _ = TryAdd(item);
        }
      });
  }
}

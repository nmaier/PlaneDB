using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
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
    private readonly PlaneDB wrappeDB;

    /// <param name="location">Directory that will store the PlaneSet</param>
    /// <param name="mode">File mode to use, supported are: CreateNew, Open (existing), OpenOrCreate</param>
    /// <param name="options">Options to use, such as the transformer, cache settings, etc.</param>
    /// <summary>Opens or creates a new PlaneSet</summary>
    public PlaneSet(DirectoryInfo location, FileMode mode, PlaneDBOptions options)
    {
      wrappeDB = new PlaneDB(location, mode, options);
    }

    /// <inheritdoc />
    public void CopyTo(Array array, int index)
    {
      ToArray().CopyTo(array, index);
    }

    /// <inheritdoc />
    public bool IsSynchronized => false;

    /// <inheritdoc />
    public object SyncRoot { get; } = new object();

    void ICollection<byte[]>.Add(byte[] item)
    {
      wrappeDB.Add(item ?? throw new ArgumentNullException(nameof(item)), Array.Empty<byte>());
    }

    /// <inheritdoc />
    public void Clear()
    {
      wrappeDB.Clear();
    }

    /// <inheritdoc />
    public bool Contains(byte[] item)
    {
      return wrappeDB.ContainsKey(item ?? throw new ArgumentNullException(nameof(item)));
    }

    void ICollection<byte[]>.CopyTo(byte[][] array, int arrayIndex)
    {
      ToArray().CopyTo(array, arrayIndex);
    }

    /// <inheritdoc />
    public bool IsReadOnly => false;

    /// <inheritdoc />
    public bool Remove(byte[] item)
    {
      return wrappeDB.TryRemove(item, out _);
    }

    /// <inheritdoc />
    public void Dispose()
    {
      wrappeDB?.Dispose();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return wrappeDB.KeysIterator.GetEnumerator();
    }

    /// <inheritdoc />
    public IEnumerator<byte[]> GetEnumerator()
    {
      return wrappeDB.KeysIterator.GetEnumerator();
    }

    /// <inheritdoc />
    public void Compact()
    {
      wrappeDB.Compact();
    }

    /// <inheritdoc />
    public long CurrentBloomBits => wrappeDB.CurrentBloomBits;

    /// <inheritdoc />
    public long CurrentDiskSize => wrappeDB.CurrentDiskSize;

    /// <inheritdoc />
    public long CurrentIndexBlockCount => wrappeDB.CurrentIndexBlockCount;

    /// <inheritdoc />
    public long CurrentRealSize => wrappeDB.CurrentRealSize;

    /// <inheritdoc />
    public int CurrentTableCount => wrappeDB.CurrentTableCount;

    /// <inheritdoc />
    public void Flush()
    {
      wrappeDB.Flush();
    }

    /// <inheritdoc />
    public DirectoryInfo Location => wrappeDB.Location;

    /// <inheritdoc />
    public void MassInsert(Action action)
    {
      wrappeDB.MassInsert(action);
    }

    /// <inheritdoc />
    public string TableSpace => wrappeDB.TableSpace;

    void IProducerConsumerCollection<byte[]>.CopyTo(byte[][] array, int index)
    {
      ToArray().CopyTo(array, index);
    }

    /// <inheritdoc />
    public byte[][] ToArray()
    {
      return wrappeDB.KeysIterator.ToArray();
    }

    /// <inheritdoc />
    public bool TryAdd(byte[] item)
    {
      return wrappeDB.TryAdd(item, Array.Empty<byte>());
    }

    /// <inheritdoc />
    public bool TryTake(out byte[] item)
    {
      var enumerator = wrappeDB.KeysIterator.GetEnumerator();
      try {
        while (enumerator.MoveNext()) {
          var current = enumerator.Current;
          if (!wrappeDB.TryRemove(current, out _)) {
            continue;
          }

          item = current;
          return true;
        }

        item = Array.Empty<byte>();
        return false;
      }
      finally {
        enumerator.Dispose();
      }
    }

    /// <inheritdoc cref="ICollection.Count" />
    public int Count => wrappeDB.Count;

    bool ISet<byte[]>.Add(byte[] item)
    {
      return wrappeDB.TryAdd(item, Array.Empty<byte>());
    }

    /// <inheritdoc />
    public void ExceptWith(IEnumerable<byte[]> other)
    {
      wrappeDB.MassInsert(() => {
        foreach (var item in other) {
          wrappeDB.TryRemove(item, out _);
        }
      });
    }

    /// <inheritdoc />
    public void IntersectWith(IEnumerable<byte[]> other)
    {
      wrappeDB.MassInsert(() => {
        Clear();
        foreach (var item in other) {
          TryAdd(item);
        }
      });
    }

    /// <inheritdoc />
    public bool IsProperSubsetOf(IEnumerable<byte[]> other)
    {
      return this.ToHashSet(new ByteArrayComparer()).IsProperSubsetOf(other);
    }

    /// <inheritdoc />
    public bool IsProperSupersetOf(IEnumerable<byte[]> other)
    {
      return this.ToHashSet(new ByteArrayComparer()).IsProperSupersetOf(other);
    }

    /// <inheritdoc />
    public bool IsSubsetOf(IEnumerable<byte[]> other)
    {
      return other.ToHashSet(new ByteArrayComparer()).IsSupersetOf(wrappeDB.KeysIterator);
    }

    /// <inheritdoc />
    public bool IsSupersetOf(IEnumerable<byte[]> other)
    {
      return other.ToHashSet(new ByteArrayComparer()).IsSubsetOf(wrappeDB.KeysIterator);
    }

    /// <inheritdoc />
    public bool Overlaps(IEnumerable<byte[]> other)
    {
      return other.Any(item => wrappeDB.ContainsKey(item));
    }

    /// <inheritdoc />
    public bool SetEquals(IEnumerable<byte[]> other)
    {
      return wrappeDB.KeysIterator.SequenceEqual(other, new ByteArrayComparer());
    }

    /// <inheritdoc />
    public void SymmetricExceptWith(IEnumerable<byte[]> other)
    {
      wrappeDB.MassInsert(() => {
        foreach (var item in other) {
          if (TryAdd(item)) {
            continue;
          }

          Remove(item);
        }
      });
    }

    /// <inheritdoc />
    public void UnionWith(IEnumerable<byte[]> other)
    {
      wrappeDB.MassInsert(() => {
        foreach (var item in other) {
          TryAdd(item);
        }
      });
    }
  }
}
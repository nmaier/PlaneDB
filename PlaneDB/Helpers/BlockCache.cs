using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using NMaier.BlockStream;

namespace NMaier.PlaneDB
{
  internal sealed class BlockCache : IDisposable
  {
    private readonly LeastRecentlyUsedDictionary<Entry, byte[]> entries;
    private readonly ArrayPool<byte> pool = ArrayPool<byte>.Create(BlockStream.BlockStream.BLOCK_SIZE, 10);

    internal BlockCache(int capacity)
    {
      entries = new LeastRecentlyUsedDictionary<Entry, byte[]>(capacity);
    }

    public void Dispose()
    {
      foreach (var kv in entries) {
        pool.Return(kv.Value);
      }
    }

    internal IBlockCache Get(ulong id)
    {
      return new SubCache(this, id);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Cache(Span<byte> block, ulong id, long offset)
    {
      entries.Set(new Entry(id, offset), block.ToArray());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Invalidate(ulong id, long offset)
    {
      if (entries.TryRemove(new Entry(id, offset), out var val)) {
        pool.Return(val);
      }
    }

    [SuppressMessage("ReSharper", "UseDeconstruction")]
    private void RemoveCache(in ulong id)
    {
      foreach (var kv in entries.ToArray()) {
        if (kv.Key.ID != id) {
          continue;
        }

        entries.Remove(kv.Key);
        pool.Return(kv.Value);
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryReadBlock(Span<byte> block, ulong id, long offset)
    {
      if (!entries.TryGetValue(new Entry(id, offset), out var val)) {
        return false;
      }

      val.AsSpan(0, block.Length).CopyTo(block);
      return true;
    }

    private readonly struct Entry : IComparable<Entry>
    {
      internal readonly ulong ID;
      private readonly long block;

      public Entry(ulong id, long block)
      {
        ID = id;
        this.block = block;
      }

      public int CompareTo(Entry other)
      {
        if (ID > other.ID) {
          return 1;
        }

        if (ID < other.ID) {
          return -1;
        }

        return (int)(block - other.block);
      }

      public override int GetHashCode()
      {
        return block.GetHashCode() ^ ID.GetHashCode();
      }
    }

    private sealed class SubCache : IBlockCache
    {
      private readonly ulong id;
      private readonly BlockCache owner;

      public SubCache(BlockCache owner, ulong id)
      {
        this.owner = owner;
        this.id = id;
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public void Cache(Span<byte> block, long offset)
      {
        owner.Cache(block, id, offset);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public void Invalidate(long offset)
      {
        owner.Invalidate(id, offset);
      }

      [MethodImpl(MethodImplOptions.AggressiveInlining)]
      public bool TryReadBlock(Span<byte> block, long offset)
      {
        return owner.TryReadBlock(block, id, offset);
      }

      public void Dispose()
      {
        owner.RemoveCache(id);
      }
    }
  }
}
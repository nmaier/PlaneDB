using System;
using System.Runtime.CompilerServices;

using NMaier.BlockStream;

// ReSharper disable UseDeconstruction
namespace NMaier.PlaneDB;

internal sealed class BlockCache : IDisposable
{
  private readonly LeastUsedDictionary<Entry, byte[]> entries;

  internal BlockCache(int capacity)
  {
    entries = new LeastUsedDictionary<Entry, byte[]>(capacity);
  }

  public void Dispose()
  {
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private void Cache(Span<byte> block, ulong id, long offset)
  {
    entries.Set(new Entry(id, offset), block.ToArray());
  }

  internal IBlockCache Get(ulong id)
  {
    return new SubCache(this, id);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  private void Invalidate(ulong id, long offset)
  {
    _ = entries.TryRemove(new Entry(id, offset));
  }

  private void RemoveCache(ulong id)
  {
    entries.RemoveIf((in Entry entry) => entry.Id == id);
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  private bool TryReadBlock(Span<byte> block, ulong id, long offset)
  {
    if (!entries.TryGetValue(new Entry(id, offset), out var val)) {
      return false;
    }

    val.AsSpan(0, block.Length).CopyTo(block);

    return true;
  }

  private sealed class SubCache(BlockCache owner, ulong id) : IBlockCache
  {
    [MethodImpl(Constants.SHORT_METHOD)]
    public void Cache(Span<byte> block, long offset)
    {
      owner.Cache(block, id, offset);
    }

    [MethodImpl(Constants.SHORT_METHOD)]
    public void Invalidate(long offset)
    {
      owner.Invalidate(id, offset);
    }

    [MethodImpl(Constants.SHORT_METHOD)]
    public bool TryReadBlock(Span<byte> block, long offset)
    {
      return owner.TryReadBlock(block, id, offset);
    }

    public void Dispose()
    {
      owner.RemoveCache(id);
    }
  }

  private readonly struct Entry(ulong id, long block) : IComparable<Entry>
  {
    internal readonly ulong Id = id;
    private readonly long block = block;

    public int CompareTo(Entry other)
    {
      var idComparison = Id.CompareTo(other.Id);

      return idComparison != 0 ? idComparison : block.CompareTo(other.block);
    }
  }
}

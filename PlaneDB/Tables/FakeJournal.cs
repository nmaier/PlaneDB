using System;

namespace NMaier.PlaneDB
{
  internal sealed class FakeJournal : IJournal
  {
    public void Dispose()
    {
    }

    public long Length => 0;

    public void Flush()
    {
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
    }

    public bool Remove(ReadOnlySpan<byte> key)
    {
      return true;
    }

    public bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      return true;
    }
  }
}
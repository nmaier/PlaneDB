using System;

namespace NMaier.PlaneDB;

internal sealed class JournalFake : IJournal
{
  public void Dispose()
  {
  }

  public long JournalLength => 0;

  public void Flush()
  {
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
  }
}

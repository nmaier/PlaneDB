using System;
using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class JournalReadOnly : IJournal
{
  [MethodImpl(Constants.SHORT_METHOD)]
  public void Dispose()
  {
  }

  public long JournalLength => 0;

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Flush()
  {
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    throw new PlaneDBReadOnlyException();
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void Remove(ReadOnlySpan<byte> key)
  {
    throw new PlaneDBReadOnlyException();
  }
}

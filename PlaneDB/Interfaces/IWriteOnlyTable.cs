using System;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  [PublicAPI]
  internal interface IWriteOnlyTable
  {
    void Flush();
    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    bool Remove(ReadOnlySpan<byte> key);
    bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
  }
}
using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal interface IWritableTable
{
  void Flush();
  void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
  void Remove(ReadOnlySpan<byte> key);
}

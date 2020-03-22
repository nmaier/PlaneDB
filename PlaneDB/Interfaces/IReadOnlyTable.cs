using System;
using System.Collections.Generic;
using JetBrains.Annotations;

namespace NMaier.PlaneDB
{
  [PublicAPI]
  internal interface IReadOnlyTable
  {
    bool ContainsKey(ReadOnlySpan<byte> key, out bool removed);
    IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate();
    IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys();
    bool TryGet(ReadOnlySpan<byte> key, out byte[]? value);
  }
}
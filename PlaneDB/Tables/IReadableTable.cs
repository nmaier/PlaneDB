using System;
using System.Collections.Generic;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal interface IReadableTable
{
  bool ContainsKey(
    ReadOnlySpan<byte> key,
    in BloomFilter.Hashes hashes,
    out bool removed);

  IEnumerable<KeyValuePair<byte[], byte[]?>> Enumerate();
  IEnumerable<KeyValuePair<byte[], byte[]?>> EnumerateKeys();
  bool TryGet(ReadOnlySpan<byte> key, in BloomFilter.Hashes hashes, out byte[]? value);
}

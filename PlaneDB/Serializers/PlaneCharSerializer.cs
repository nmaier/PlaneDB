using System;
using System.Buffers.Binary;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
[PublicAPI]
public sealed class PlaneCharSerializer : IPlaneSerializer<char>
{
  /// <inheritdoc />
  public char Deserialize(ReadOnlySpan<byte> bytes)
  {
    return (char)BinaryPrimitives.ReadUInt16LittleEndian(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in char obj)
  {
    var rv = new byte[sizeof(char)];
    BinaryPrimitives.WriteUInt16LittleEndian(rv, obj);

    return rv;
  }
}

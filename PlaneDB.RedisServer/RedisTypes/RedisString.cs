using System;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace NMaier.PlaneDB.RedisTypes;

internal sealed class RedisString(byte[] value, long expires = -1)
  : RedisValue(RedisValueType.String, expires), IComparable<RedisString>,
    IEquatable<RedisString>
{
  private static readonly IPlaneByteArrayComparer comparer =
    PlaneByteArrayComparer.Default;

  internal readonly byte[] Value = value;
  public long IntValue => StringValue.AsLong();

  public int CompareTo(RedisString? other)
  {
    return other == null
      ? Comparer<byte[]?>.Default.Compare(Value, null)
      : comparer.Compare(Value, other.Value);
  }

  public bool Equals(RedisString? other)
  {
    return other != null && comparer.Equals(Value, other.Value);
  }

  public override bool Equals(object? obj)
  {
    return obj is RedisString s && comparer.Equals(Value, s.Value);
  }

  public override int GetHashCode()
  {
    return comparer.GetHashCode(Value);
  }

  internal override byte[] Serialize()
  {
    var rv = new byte[1 + sizeof(long) + Value.Length];
    rv[0] = (byte)RedisValueType.String;
    BinaryPrimitives.WriteInt64LittleEndian(rv.AsSpan(1), Expires);
    Value.AsSpan().CopyTo(rv.AsSpan(1 + sizeof(long)));

    return rv;
  }
}

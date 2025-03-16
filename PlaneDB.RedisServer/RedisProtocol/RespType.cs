using System;
using System.Globalization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal abstract class RespType
{
  internal bool IsNull => this is RespNullString or RespNullArray;

  internal RespType[] AsArray()
  {
    return this is RespArray a
      ? a.Elements
      : throw new InvalidCastException("Not an array");
  }

  public byte[] AsBytes()
  {
    return this switch {
      RespBulkString b => b.Value,
      _ => throw new InvalidCastException("Not a byte buffer")
    };
  }

  internal long AsLong()
  {
    return this switch {
      RespBulkString respBulkString => long.Parse(
        respBulkString.AsString(),
        CultureInfo.InvariantCulture),
      RespInteger respInteger => respInteger.Value,
      RespString respString => long.Parse(respString.Value, CultureInfo.InvariantCulture),
      _ => throw new InvalidCastException("Not a value that can be cast to an integer")
    };
  }

  internal string? AsNullableString()
  {
    return this switch {
      RespString s => s.Value,
      RespBulkString b => b.ToString(),
      RespNullString => null,
      _ => throw new InvalidCastException("Not a nullable string")
    };
  }

  internal string AsString()
  {
    return this switch {
      RespString s => s.Value,
      RespBulkString b => b.ToString(),
      _ => throw new InvalidCastException("Not a string")
    };
  }
}

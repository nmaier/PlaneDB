using System;
using System.Text;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <inheritdoc />
/// <summary>
///   A new string serializer using the specified encoding
/// </summary>
/// <param name="encoding">Encoding to use</param>
[PublicAPI]
public sealed class PlaneStringSerializer(Encoding encoding) : IPlaneSerializer<string>
{
  /// <summary>
  ///   Default instance (utf-8)
  /// </summary>
  public static readonly PlaneStringSerializer Default = new();

  /// <summary>
  ///   A new string serializer using UTF-8
  /// </summary>
  public PlaneStringSerializer() : this(Encoding.UTF8)
  {
  }

  /// <inheritdoc />
  public string Deserialize(ReadOnlySpan<byte> bytes)
  {
    return bytes.Length == 0 ? string.Empty : encoding.GetString(bytes);
  }

  /// <inheritdoc />
  public byte[] Serialize(in string obj)
  {
    return obj.Length == 0 ? [] : encoding.GetBytes(obj);
  }
}

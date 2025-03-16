using System.Text;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespBulkString(byte[] value) : RespType
{
  public readonly byte[] Value = value;

  public RespBulkString(string value) : this(Encoding.UTF8.GetBytes(value))
  {
  }

  public int Length => Value.Length;

  public override string ToString()
  {
    return Encoding.UTF8.GetString(Value);
  }
}

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespString(string value) : RespType
{
  public static readonly RespString OK = new("OK");
  public readonly string Value = value;

  public override string ToString()
  {
    return Value;
  }
}

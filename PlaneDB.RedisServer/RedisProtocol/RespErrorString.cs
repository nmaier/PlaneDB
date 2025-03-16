using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespErrorString(string value) : RespType
{
  public readonly string Value = value;

  public override string ToString()
  {
    return Value;
  }
}

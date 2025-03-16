using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespNullArray : RespType
{
  internal static readonly RespNullArray Value = new();

  public override string ToString()
  {
    return "<nil>";
  }
}

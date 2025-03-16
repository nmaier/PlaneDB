using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespNullString : RespType
{
  internal static readonly RespNullString Value = new();

  public override string ToString()
  {
    return "(nil)";
  }
}

using System.Globalization;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespInteger : RespType
{
  public readonly long Value;

  public RespInteger(int v)
  {
    Value = v;
  }

  public RespInteger(long v)
  {
    Value = v;
  }

  public override string ToString()
  {
    return Value.ToString(CultureInfo.InvariantCulture);
  }
}

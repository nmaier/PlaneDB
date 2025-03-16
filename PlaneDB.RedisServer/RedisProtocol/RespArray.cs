using System.Collections.Generic;
using System.Linq;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespArray : RespType
{
  public readonly RespType[] Elements;

  public RespArray(IEnumerable<RespType> elements)
  {
    Elements = elements.ToArray();
  }

  public RespArray(params RespType[] elements)
  {
    Elements = elements;
  }

  public RespType this[int i] => Elements[i];
  public long Length => Elements.Length;

  public override string ToString()
  {
    return $"[{string.Join(", ", Elements.Select(e => e.ToString()))}]";
  }
}

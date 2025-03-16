using System;
using System.IO;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal class RespException : IOException
{
  public RespException(string message) : base(message) { }

  public RespException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }
}

using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespProtocolException : RespException
{
  public RespProtocolException(string message) : base(message) { }

  public RespProtocolException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }
}

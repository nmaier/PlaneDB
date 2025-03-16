using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB.RedisProtocol;

[PublicAPI]
internal sealed class RespResponseException : RespException
{
  public static readonly RespResponseException WrongNumberOfArguments =
    new("Wrong number of arguments");

  internal RespResponseException(string message) : base(message) { }

  internal RespResponseException(string message, Exception innerException) : base(
    message,
    innerException)
  {
  }
}

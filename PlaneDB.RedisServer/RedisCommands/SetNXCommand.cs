using NMaier.PlaneDB.RedisProtocol;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class SetNXCommand : IRedisCommand
{
  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    return SetCommand.ApplyNX(
             client,
             args[0].AsBytes(),
             args[1].AsBytes(),
             new SetCommand.Options { Mode = SetCommand.Mode.NX }) ==
           RespString.OK
      ? new RespInteger(1)
      : new RespInteger(0);
  }

  public int MaxArgs => 2;
  public int MinArgs => 2;
}

using System.Linq;

using NMaier.PlaneDB.RedisProtocol;
using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class KeysCommand : IRedisCommand
{
  private static readonly byte[] all = [
    (byte)'*'
  ];

  private static readonly IPlaneByteArrayComparer cmp = PlaneByteArrayComparer.Default;

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var pattern = args[0].AsBytes();
    var keyiter = client.KeysIterator;
    keyiter = !cmp.Equals(pattern, all)
      ? keyiter.Where(
        k => k.KeyType == RedisKeyType.Normal &&
             RedisExtensions.StringMatch(k.KeyBytes, pattern))
      : keyiter.Where(k => k.KeyType == RedisKeyType.Normal);
    var resp = keyiter.Where(k => client.TryGetValue(k, out _))
      .Select(k => new RespBulkString(k.KeyBytes))
      .Cast<RespType>()
      .ToList();

    return new RespArray(resp);
  }

  public int MaxArgs => 1;
  public int MinArgs => 1;
}

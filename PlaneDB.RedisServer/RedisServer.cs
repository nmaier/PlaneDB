using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using NMaier.PlaneDB.RedisTypes;

namespace NMaier.PlaneDB;

[SuppressMessage(
  "Design",
  "CA1001:Types that own disposable fields should be disposable")]
internal sealed class RedisServer(
  IPlaneDB<byte[], byte[]> db,
  PlaneDBRemoteOptions dbRemoteOptions) : BaseServer(dbRemoteOptions)
{
  private readonly IPlaneDB<RedisKey, RedisValue> db =
    new TypedPlaneDB<RedisKey, RedisValue>(
      new RedisKeySerializer(),
      new RedisValueSerializer(),
      db);

  protected override async Task Process(
    TcpClient client,
    Stream stream,
    CancellationToken token)
  {
    var c = new RedisServerClient(db, stream, stream, DBRemoteOptions.AuthToken, token);
    await c.Serve().ConfigureAwait(false);
  }
}

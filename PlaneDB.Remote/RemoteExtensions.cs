using System.Threading;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Remote Extensions for PlaneDB
/// </summary>
[PublicAPI]
public static class RemoteExtensions
{
  /// <summary>
  ///   Serve this PlaneDB with the PlaneDB.Remote protocol
  /// </summary>
  /// <param name="options">Remote options</param>
  /// <param name="token">Cancellation token to stop server again</param>
  /// <remarks>This method does not return until the token is cancelled</remarks>
  public static IPlaneDBRemote Serve(
    this PlaneDBRemoteOptions options,
    CancellationToken token)
  {
    var server = new PlaneDBServer(options);

    return server.Serve(token);
  }
}

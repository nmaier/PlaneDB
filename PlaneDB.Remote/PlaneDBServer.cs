using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBServer(PlaneDBRemoteOptions dbRemoteOptions)
  : BaseServer(dbRemoteOptions)
{
  internal const byte AUTH_PROTOCOL = 5;

  protected override async Task Process(
    TcpClient client,
    Stream stream,
    CancellationToken token)
  {
    try {
      {
        // Do auth twice to reduce likelihood of nonce reuse 
        using var authSource = CancellationTokenSource.CreateLinkedTokenSource(token);
        authSource.CancelAfter(TimeSpan.FromSeconds(20));

        if (!await HandleAuth(stream, authSource.Token).ConfigureAwait(false)) {
          throw new IOException("Invalid buggy authentication state");
        }

        if (!await HandleAuth(stream, authSource.Token).ConfigureAwait(false)) {
          throw new IOException("Invalid buggy authentication state");
        }
      }

      var moniker = await stream.ReadArray(1024, token).ConfigureAwait(false) ??
                    throw new IOException("No moniker provided!");
      var conn = new PlaneDBConnection(
        DBRemoteOptions.GetDB(Encoding.UTF8.GetString(moniker)));
      await conn.Process(stream, token).ConfigureAwait(false);
    }
    catch (Exception ex) when (client.Connected) {
      await stream.WriteException(ex, token).ConfigureAwait(false);
      await stream.FlushAsync(token).ConfigureAwait(false);

      throw;
    }
  }

  private async Task<bool> HandleAuth(Stream stream, CancellationToken token)
  {
    // Stupidly stupid auth
    var ourNonce = new byte[PlaneDBRemoteOptions.NONCE_LENGTH];
    PlaneDBRemoteOptions.FillNonce(ourNonce);

    byte[]? ourChallenge = null;
    byte[]? theirResponse = null;
    byte[]? ourResponse = null;
    try {
      ourChallenge = DBRemoteOptions.ComputeSecret(ourNonce);
      await stream.WriteArray(
          [
            AUTH_PROTOCOL
          ],
          token)
        .ConfigureAwait(false);
      await stream.WriteArray(ourNonce, token).ConfigureAwait(false);
      await stream.FlushAsync(token).ConfigureAwait(false);

      var theirNonce = await stream.ReadArray(token).ConfigureAwait(false);
      if (theirNonce is not { Length: PlaneDBRemoteOptions.NONCE_LENGTH }) {
        throw new IOException("Truncated nonce");
      }

      theirResponse = await stream.ReadArray(ourChallenge.Length, token)
        .ConfigureAwait(false);
      if (theirResponse == null || theirResponse.Length != ourChallenge.Length) {
        throw new IOException("Truncated auth");
      }

      if (!ourChallenge.ConstantTimeEquals(theirResponse)) {
        throw new IOException("Invalid auth");
      }

      ourResponse = DBRemoteOptions.ComputeSecret(theirNonce);
      await stream.WriteArray(ourResponse, token).ConfigureAwait(false);
      await stream.FlushAsync(token).ConfigureAwait(false);

      return true;
    }
    finally {
      ourChallenge?.AsSpan().Clear();
      ourResponse?.AsSpan().Clear();
      theirResponse?.AsSpan().Clear();
    }
  }
}

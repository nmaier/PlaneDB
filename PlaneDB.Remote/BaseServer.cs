using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace NMaier.PlaneDB;

internal abstract class BaseServer(PlaneDBRemoteOptions dbRemoteOptions)
{
  private readonly HashSet<ConfiguredTaskAwaitable> clientTasks = [];
  protected readonly PlaneDBRemoteOptions DBRemoteOptions = dbRemoteOptions;

  private async Task HandleClient(TcpClient client, CancellationToken token)
  {
    try {
      //client.NoDelay = true;
      client.ReceiveBufferSize = 1 << 17;
      client.SendBufferSize = 1 << 17;

      async Task<Stream> InitTLS()
      {
        var tlsStream = new SslStream(client.GetStream(), false);
        try {
          var tlsOpts = new SslServerAuthenticationOptions {
            AllowRenegotiation = true,
            ClientCertificateRequired = false,
            ServerCertificate = DBRemoteOptions.Certificate,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck
          };
          await tlsStream.AuthenticateAsServerAsync(tlsOpts, token).ConfigureAwait(false);

          return tlsStream;
        }
        catch {
          try {
            await tlsStream.DisposeAsync().ConfigureAwait(false);
          }
          catch {
            // ignored
          }

          throw;
        }
      }

      await using var stream = DBRemoteOptions.Certificate switch {
        null => client.GetStream(),
        _ => await InitTLS().ConfigureAwait(false)
      };

      await Process(client, stream, token);
    }
    catch {
      // ignored for now
    }
    finally {
      try {
        client.Close();
      }
      catch {
        // ignored
      }
    }
  }

  protected abstract Task Process(
    TcpClient client,
    Stream stream,
    CancellationToken token);

  private async Task Run(TcpListener listener, CancellationToken token)
  {
    await using var reg = token.Register(listener.Stop);

    try {
      while (!token.IsCancellationRequested) {
#if NET6_0_OR_GREATER
        var client = await listener.AcceptTcpClientAsync(token);
#else
          var client = await listener.AcceptTcpClientAsync();
#endif

        _ = token.Register(Closer);

        var clientTask = Task.Run(HandleClientLocal, token).ConfigureAwait(false);
        clientTask.GetAwaiter()
          .OnCompleted(
            () => {
              lock (clientTasks) {
                _ = clientTasks.Remove(clientTask);
              }
            });
        lock (clientTasks) {
          _ = clientTasks.Add(clientTask);
        }

        continue;

        void Closer()
        {
          client.Close();
        }

        Task HandleClientLocal()
        {
          return HandleClient(client, token);
        }
      }
    }
    finally {
      ConfiguredTaskAwaitable[] pending;
      lock (clientTasks) {
        pending = [..clientTasks];
      }

      foreach (var task in pending) {
        try {
          await task;
        }
        catch {
          // ignored
        }
      }
    }
  }

  public IPlaneDBRemote Serve(CancellationToken token)
  {
    var remote = new PlaneDBRemote(token);
    ServeInternal(remote);

    return remote;
  }

  private void ServeInternal(PlaneDBRemote remote)
  {
    var token = remote.Token;
    var listener =
      new TcpListener(DBRemoteOptions.Address, DBRemoteOptions.Port) {
        Server = { NoDelay = true }
      };
    listener.Start();
    var endpoint = (IPEndPoint)listener.LocalEndpoint;
    remote.Port = endpoint.Port;
    remote.Address = endpoint.Address;
    remote.ServerTask = Run(listener, token);
  }

  private sealed class PlaneDBRemote : IPlaneDBRemote
  {
    private readonly CancellationTokenSource linked;
    private readonly CancellationTokenSource source = new();

    public PlaneDBRemote(CancellationToken token)
    {
      linked = CancellationTokenSource.CreateLinkedTokenSource(source.Token, token);
    }

    public CancellationToken Token => linked.Token;

    public void Dispose()
    {
      StopServer();
      ServerTask?.Dispose();
      linked.Dispose();
      source.Dispose();
    }

    public IPAddress Address
    {
      get;
      internal set;
    } = IPAddress.None;

    public int Port
    {
      get;
      internal set;
    }

    public Task? ServerTask { get; internal set; }

    public void StopServer()
    {
      source.Cancel();
      Wait();
    }

    public void Wait()
    {
      try {
        ServerTask?.ConfigureAwait(false).GetAwaiter().GetResult();
      }
      catch {
        // ignored
      }
    }
  }
}

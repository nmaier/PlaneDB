using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBRemoteClient : IPlaneDB<byte[], byte[]>
{
  private sealed record ClientEntry(
    TcpClient Client,
    Stream Stream,
    PlaneProtocolRandom CanaryRandom)
  {
    internal void Close()
    {
      try {
        Stream.Close();
      }
      catch {
        // ignored
      }

      try {
        Client.Close();
      }
      catch {
        // ignored
      }
    }
  }

  private readonly ConcurrentBag<ClientEntry> clients = [];
  private readonly PlaneDBRemoteOptions dbRemoteOptions;
  private readonly string moniker;
  private readonly CancellationTokenSource source;
  private CancellationToken Token => source.Token;

  internal PlaneDBRemoteClient(
    PlaneDBRemoteOptions dbRemoteOptions,
    string moniker,
    CancellationToken token)
  {
    this.dbRemoteOptions = dbRemoteOptions;
    this.moniker = moniker;
    source = CancellationTokenSource.CreateLinkedTokenSource(token);
  }

  public void Add(KeyValuePair<byte[], byte[]> item)
  {
    if (!TryAdd(item.Key, item.Value)) {
      throw new ArgumentException("Key exists");
    }
  }

  public void Clear()
  {
    _ = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Clear, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      true);
  }

  public bool Contains(KeyValuePair<byte[], byte[]> item)
  {
    return ContainsKey(item.Key);
  }

  public void CopyTo(KeyValuePair<byte[], byte[]>[] array, int arrayIndex)
  {
    throw new NotSupportedException();
  }

  public int Count => GetCount();
  public bool IsReadOnly => GetReadOnly();

  private bool GetReadOnly()
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.ReadOnly, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      true);
  }

  public bool Remove(KeyValuePair<byte[], byte[]> item)
  {
    return Remove(item.Key);
  }

  public void Add(byte[] key, byte[] value)
  {
    if (!TryAdd(key, value)) {
      throw new ArgumentException("Key exists");
    }
  }

  public bool ContainsKey(byte[] key)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.ContainsKey, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      true);
  }

  public byte[] this[byte[] key]
  {
    get => !TryGetValue(key, out var value) ? throw new KeyNotFoundException() : value;
    set => SetValue(key, value);
  }

  public ICollection<byte[]> Keys
  {
    get
    {
      var iterator = KeysIterator;

      return iterator as ICollection<byte[]> ?? iterator.ToArray();
    }
  }

  public bool Remove(byte[] key)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Remove, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      false);
  }

  public bool TryGetValue(byte[] key, out byte[] value)
  {
    byte[] ores = null!;
    var rv = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TryGetValue, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        var res = await stream.ReadArray(Token).ConfigureAwait(false);
        if (res != null) {
          ores = res;
        }

        return res != null;
      },
      true);
    value = ores;

    return rv;
  }

  public ICollection<byte[]> Values => GetList().Select(kv => kv.Value).ToArray();

  public void Dispose()
  {
    source.Cancel();
    source.Dispose();
  }

  [MustDisposeResource]
  IEnumerator IEnumerable.GetEnumerator()
  {
    return GetEnumerator();
  }

  [MustDisposeResource]
  public IEnumerator<KeyValuePair<byte[], byte[]>> GetEnumerator()
  {
    return GetList().GetEnumerator();
  }

  private IEnumerable<KeyValuePair<byte[], byte[]>> GetList()
  {
    var client = EnsureClient().Result;
    var stream = client.Stream;
    try {
      stream.WriteCommand(CommandCode.Enumerate, client.CanaryRandom, Token).Wait(Token);
      stream.FlushAsync(Token).Wait(Token);
    }
    catch {
      client.Close();

      throw;
    }

    for (;;) {
      KeyValuePair<byte[], byte[]> rv;
      try {
        var key = stream.ReadArray(Token).Result;
        if (key == null) {
          break;
        }

        rv = new KeyValuePair<byte[], byte[]>(key, stream.ReadArray(Token).Result!);
      }
      catch {
        client.Close();

        throw;
      }

      yield return rv;
    }

    clients.Add(client);
  }

  private IEnumerable<byte[]> GetKeys()
  {
    var client = EnsureClient().Result;
    var stream = client.Stream;
    try {
      stream.WriteCommand(CommandCode.EnumerateKeys, client.CanaryRandom, Token)
        .Wait(Token);
      stream.FlushAsync(Token).Wait(Token);
    }
    catch {
      client.Close();

      throw;
    }

    for (;;) {
      byte[]? key;
      try {
        key = stream.ReadArray(Token).Result;
        if (key == null) {
          break;
        }
      }
      catch {
        client.Close();

        throw;
      }

      yield return key;
    }

    clients.Add(client);
  }

  public IPlaneDB<byte[], byte[]> BaseDB => throw new NotSupportedException();

  public void Compact(CompactionMode mode = CompactionMode.Normal)
  {
    throw new NotSupportedException();
  }

  public long CurrentBloomBits => throw new NotSupportedException();
  public long CurrentDiskSize => throw new NotSupportedException();
  public long CurrentIndexBlockCount => throw new NotSupportedException();
  public long CurrentRealSize => throw new NotSupportedException();
  public int CurrentTableCount => throw new NotSupportedException();

  public void Flush()
  {
    _ = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Flush, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      false);
  }

  public DirectoryInfo Location => throw new NotSupportedException();

  public void MassInsert(Action action)
  {
    throw new NotSupportedException();
  }

  public TResult MassInsert<TResult>(Func<TResult> action)
  {
    throw new NotSupportedException();
  }

  public void MassRead(Action action)
  {
    throw new NotSupportedException();
  }

  public TResult MassRead<TResult>(Func<TResult> action)
  {
    throw new NotSupportedException();
  }

  public PlaneOptions Options => throw new NotSupportedException();
  public string TableSpace => GetTableSpace();

  private string GetTableSpace()
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TableSpace, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return Encoding.UTF8.GetString(
          await stream.ReadArray(Token).ConfigureAwait(false) ?? []);
      },
      true);
  }

  public async IAsyncEnumerable<byte[]> GetKeysIteratorAsync(
    [EnumeratorCancellation] CancellationToken token)
  {
    var client = await EnsureClient().ConfigureAwait(false);
    var stream = client.Stream;
    try {
      await stream.WriteCommand(CommandCode.EnumerateKeys, client.CanaryRandom, Token)
        .ConfigureAwait(false);
      await stream.FlushAsync(Token).ConfigureAwait(false);
    }
    catch {
      client.Close();

      throw;
    }

    for (;;) {
      byte[]? key;
      try {
        key = await stream.ReadArray(Token).ConfigureAwait(false);
        if (key == null) {
          break;
        }
      }
      catch {
        client.Close();

        throw;
      }

      yield return key;
    }

    clients.Add(client);
  }

  public byte[] AddOrUpdate(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.ValueFactory addValueFactory,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactory updateValueFactory)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.AddOrUpdate, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        var value = await stream.ReadBool(Token).ConfigureAwait(false) switch {
          false => updateValueFactory(
            await stream.ReadArray(Token).ConfigureAwait(false) ??
            throw new InvalidOperationException()),
          _ => addValueFactory()
        };
        await stream.WriteArray(value, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        _ = await stream.ReadBool(Token).ConfigureAwait(false);

        return value;
      },
      false);
  }

  public byte[] AddOrUpdate(
    byte[] key,
    byte[] addValue,
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactory updateValueFactory)
  {
    return AddOrUpdate(key, () => addValue, updateValueFactory);
  }

  public byte[] AddOrUpdate<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithArg<TArg> addValueFactory,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.UpdateValueFactoryWithArg<TArg> updateValueFactory,
    TArg factoryArgument)
  {
    return AddOrUpdate(
      key,
      () => addValueFactory(factoryArgument),
      (in byte[] existing) => updateValueFactory(existing, factoryArgument));
  }

  public void CopyTo(IDictionary<byte[], byte[]> destination)
  {
    throw new NotSupportedException();
  }

  public byte[] GetOrAdd(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.ValueFactory valueFactory)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.GetOrAdd, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        var value = await stream.ReadArray(Token).ConfigureAwait(false);
        if (value != null) {
          return value;
        }

        value = valueFactory();
        await stream.WriteArray(value, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        _ = await stream.ReadBool(Token).ConfigureAwait(false);

        return value;
      },
      false);
  }

  public byte[] GetOrAdd(byte[] key, byte[] value)
  {
    return GetOrAdd(key, () => value);
  }

  public byte[] GetOrAdd(byte[] key, byte[] value, out bool added)
  {
    var innerAdded = false;
    var rv = GetOrAdd(
      key,
      () => {
        innerAdded = true;

        return value;
      });
    added = innerAdded;

    return rv;
  }

  public byte[] GetOrAdd<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    return GetOrAdd(key, () => valueFactory(factoryArgument));
  }

  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    IEnumerable<KeyValuePair<byte[], byte[]>> keysAndDefaults)
  {
    throw new NotSupportedException();
  }

  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    IEnumerable<byte[]> keys,
    byte[] value)
  {
    throw new NotSupportedException();
  }

  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange(
    IEnumerable<byte[]> keys,
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithKey valueFactory)
  {
    throw new NotSupportedException();
  }

  public IEnumerable<KeyValuePair<byte[], byte[]>> GetOrAddRange<TArg>(
    IEnumerable<byte[]> keys,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.ValueFactoryWithKeyAndArg<TArg> valueFactory,
    TArg factoryArgument)
  {
    throw new NotSupportedException();
  }

  public IEnumerable<byte[]> KeysIterator => GetKeys();

  public void RegisterMergeParticipant(
    IPlaneDBMergeParticipant<byte[], byte[]> participant)
  {
    throw new NotSupportedException();
  }

  public void UnregisterMergeParticipant(
    IPlaneDBMergeParticipant<byte[], byte[]> participant)
  {
    throw new NotSupportedException();
  }

#pragma warning disable 67
  public event EventHandler<IPlaneDB<byte[], byte[]>>? OnFlushMemoryTable;
  public event EventHandler<IPlaneDB<byte[], byte[]>>? OnMergedTables;
#pragma warning restore

  public void SetValue(byte[] key, byte[] value)
  {
    _ = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Set, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.WriteArray(value, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      true);
  }

  public bool TryAdd(byte[] key, byte[] value)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TryAdd, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.WriteArray(value, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      false);
  }

  public bool TryAdd(byte[] key, byte[] value, out byte[] existing)
  {
    byte[]? ores = null;
    var rv = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TryAdd2, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.WriteArray(value, Token).ConfigureAwait(false);
        ores = await stream.ReadArray(Token).ConfigureAwait(false);

        return ores == null;
      },
      false);
    existing = ores!;

    return rv;
  }

  public (long, long) TryAdd(IEnumerable<KeyValuePair<byte[], byte[]>> pairs)
  {
    throw new NotImplementedException();
  }

  public bool TryRemove(byte[] key, out byte[] value)
  {
    byte[]? ores = null;
    var rv = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TryRemove, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);
        ores = await stream.ReadArray(Token).ConfigureAwait(false);

        return ores != null;
      },
      false);
    value = ores!;

    return rv;
  }

  public long TryRemove(IEnumerable<byte[]> keys)
  {
    throw new NotImplementedException();
  }

  public bool TryUpdate(byte[] key, byte[] newValue, byte[] comparisonValue)
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.TryUpdate, canarySource, Token)
          .ConfigureAwait(false);
        await stream.WriteArray(key, Token).ConfigureAwait(false);
        await stream.WriteArray(newValue, Token).ConfigureAwait(false);
        await stream.WriteArray(comparisonValue, Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      false);
  }

  public bool TryUpdate(
    byte[] key,
    [InstantHandle] IPlaneDictionary<byte[], byte[]>.TryUpdateFactory updateFactory)
  {
    throw new NotImplementedException();
  }

  public bool TryUpdate<TArg>(
    byte[] key,
    [InstantHandle]
    IPlaneDictionary<byte[], byte[]>.TryUpdateFactoryWithArg<TArg> updateFactory,
    TArg arg)
  {
    throw new NotImplementedException();
  }

  public void Fail()
  {
    _ = WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Fail, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadBool(Token).ConfigureAwait(false);
      },
      true);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private async Task<ClientEntry> EnsureClient()
  {
    if (!clients.TryTake(out var rv)) {
      return await OpenClient().ConfigureAwait(false);
    }

    if (rv.Client.Connected) {
      return rv;
    }

    rv.Close();

    return await OpenClient().ConfigureAwait(false);
  }

  private int GetCount()
  {
    return WithClient(
      async (stream, canarySource) => {
        await stream.WriteCommand(CommandCode.Count, canarySource, Token)
          .ConfigureAwait(false);
        await stream.FlushAsync(Token).ConfigureAwait(false);

        return await stream.ReadInt(Token).ConfigureAwait(false);
      },
      true);
  }

  private async Task<bool> HandleAuth(Stream s, CancellationToken token)
  {
    var protocol = await s.ReadArray(1, token).ConfigureAwait(false);
    if (protocol == null || protocol[0] != PlaneDBServer.AUTH_PROTOCOL) {
      throw new IOException("Unsupported protocol");
    }

    var ourNonce = new byte[PlaneDBRemoteOptions.NONCE_LENGTH];
    PlaneDBRemoteOptions.FillNonce(ourNonce);
    await s.WriteArray(ourNonce, token).ConfigureAwait(false);

    var theirNonce = await s.ReadArray(PlaneDBRemoteOptions.NONCE_LENGTH, token)
      .ConfigureAwait(false);
    if (theirNonce is not { Length: PlaneDBRemoteOptions.NONCE_LENGTH }) {
      throw new IOException("Invalid nonce");
    }

    byte[]? ourResponse = null;
    byte[]? ourChallenge = null;
    byte[]? theirResponse = null;
    try {
      ourResponse = dbRemoteOptions.ComputeSecret(theirNonce);
      await s.WriteArray(ourResponse, token).ConfigureAwait(false);
      await s.FlushAsync(token).ConfigureAwait(false);
      ourChallenge = dbRemoteOptions.ComputeSecret(ourNonce);
      theirResponse = await s.ReadArray(ourChallenge.Length, token).ConfigureAwait(false);

      return theirResponse == null
        ? throw new IOException("Truncated auth")
        :
        !ourChallenge.ConstantTimeEquals(theirResponse)
          ?
          throw new IOException("Invalid auth")
          : true;
    }
    finally {
      ourResponse?.AsSpan().Clear();
      ourChallenge?.AsSpan().Clear();
      theirResponse?.AsSpan().Clear();
    }
  }

  private async Task<ClientEntry> OpenClient()
  {
    var client = new TcpClient {
      ReceiveBufferSize = 1 << 17,
      SendBufferSize = 1 << 17
    };

    try {
      await client.ConnectAsync(dbRemoteOptions.Address, dbRemoteOptions.Port, Token)
        .ConfigureAwait(false);

      var stream = dbRemoteOptions.Certificate switch {
        null => client.GetStream(),
        _ => await InitTLS(client, dbRemoteOptions.Certificate).ConfigureAwait(false)
      };
      try {
        using var authTokenSource =
          CancellationTokenSource.CreateLinkedTokenSource(Token);
        authTokenSource.CancelAfter(TimeSpan.FromSeconds(20));

        if (!await HandleAuth(stream, authTokenSource.Token).ConfigureAwait(false)) {
          throw new IOException("Invalid buggy authentication state");
        }

        if (!await HandleAuth(stream, authTokenSource.Token).ConfigureAwait(false)) {
          throw new IOException("Invalid buggy authentication state");
        }

        await stream.WriteArray(Encoding.UTF8.GetBytes(moniker), Token)
          .ConfigureAwait(false);
        var canarySource =
          new PlaneProtocolRandom(await stream.ReadInt(Token).ConfigureAwait(false));

        var rv = new ClientEntry(client, stream, canarySource);
        _ = source.Token.Register(rv.Close);

        return rv;
      }
      catch {
        await stream.DisposeAsync().ConfigureAwait(false);

        throw;
      }
    }
    catch {
      client.Dispose();

      throw;
    }
  }

  private static async Task<Stream> InitTLS(
    [InstantHandle] TcpClient tcpClient,
    X509Certificate2 certificate)
  {
    var tlsStream = new SslStream(
      tcpClient.GetStream(),
      false,
      (_, remoteCertificate, _, errors) => errors switch {
        SslPolicyErrors.None => true,
        SslPolicyErrors.RemoteCertificateChainErrors => remoteCertificate?.Equals(
                                                          certificate) ??
                                                        false,
        _ => false
      });
    var targetHost = certificate.GetNameInfo(X509NameType.SimpleName, false);
    await tlsStream.AuthenticateAsClientAsync(targetHost).ConfigureAwait(false);

    return tlsStream;
  }

  private T WithClient<T>(Func<Stream, PlaneProtocolRandom, Task<T>> func, bool retryable)
  {
    for (var attempt = 0;; ++attempt) {
      try {
        return Task.Run(
            async () => {
              var client = await EnsureClient().ConfigureAwait(false);
              try {
                var rv = await func(client.Stream, client.CanaryRandom)
                  .ConfigureAwait(false);
                clients.Add(client);

                return rv;
              }
              catch (SocketException) {
                client.Close();

                // Proactively close all cached clients
                while (clients.TryTake(out client)) {
                  try {
                    client.Close();
                  }
                  catch {
                    // ignored
                  }
                }

                throw;
              }
              catch {
                client.Close();

                throw;
              }
            },
            Token)
          .ConfigureAwait(false)
          .GetAwaiter()
          .GetResult();
      }
      catch (SocketException) when (retryable && attempt < 10) {
        // ignored
      }
    }
  }

  public async IAsyncEnumerator<KeyValuePair<byte[], byte[]>> GetAsyncEnumerator(
    CancellationToken cancellationToken = new())
  {
    var client = await EnsureClient().ConfigureAwait(false);
    var stream = client.Stream;
    try {
      await stream.WriteCommand(CommandCode.Enumerate, client.CanaryRandom, Token)
        .ConfigureAwait(false);
      await stream.FlushAsync(Token).ConfigureAwait(false);
    }
    catch {
      client.Close();

      throw;
    }

    for (;;) {
      KeyValuePair<byte[], byte[]> rv;
      try {
        var key = await stream.ReadArray(Token).ConfigureAwait(false);
        if (key == null) {
          break;
        }

        rv = new KeyValuePair<byte[], byte[]>(
          key,
          (await stream.ReadArray(Token).ConfigureAwait(false))!);
      }
      catch {
        client.Close();

        throw;
      }

      yield return rv;
    }

    clients.Add(client);
  }
}

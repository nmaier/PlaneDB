using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Fancy options for hosting or connecting to a remote PlaneDB
/// </summary>
[PublicAPI]
public sealed class PlaneDBRemoteOptions : IDisposable
{
  internal const int NONCE_LENGTH = 64;
  private static readonly RandomNumberGenerator rnd = RandomNumberGenerator.Create();

  internal static void FillNonce(byte[] nonce)
  {
    if (nonce is not { Length: NONCE_LENGTH }) {
      throw new IOException("Invalid nonce buffer");
    }

    rnd.GetBytes(nonce, 0, nonce.Length - sizeof(long));
    BinaryPrimitives.WriteInt64LittleEndian(
      nonce.AsSpan(nonce.Length - sizeof(long)),
      DateTime.UtcNow.Ticks);
  }

  /// <summary>
  ///   Secret auth token
  /// </summary>
  public readonly string AuthToken;

  /// <summary>
  ///   Server certificate to expect or use, if any.
  /// </summary>
  public readonly X509Certificate2? Certificate;

  private readonly Dictionary<string, IPlaneDB<byte[], byte[]>> databases =
    new(StringComparer.Ordinal);

  /// <summary>
  ///   Basic options with potential self-signed certificate
  /// </summary>
  /// <param name="authToken">Authentication token (pre-shared key, password)</param>
  /// <param name="createSelfSigned">Create a self-signed certificate</param>
  /// <remarks>It's left as an exercise to the reader to distribute this certificate</remarks>
  public PlaneDBRemoteOptions(string authToken, bool createSelfSigned = false)
  {
    AuthToken = authToken;
    if (!createSelfSigned) {
      return;
    }

    var ecDsa = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    var req = new CertificateRequest(
      "CN=planeSelfSigned",
      ecDsa,
      HashAlgorithmName.SHA384);
    var oids = new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") };
    req.CertificateExtensions.Add(
      new X509KeyUsageExtension(
        X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
        true));
    req.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(oids, true));
    Certificate = req.CreateSelfSigned(
      DateTimeOffset.UtcNow.AddDays(-1),
      DateTimeOffset.UtcNow.AddDays(720));
    Certificate = new X509Certificate2(Certificate.Export(X509ContentType.Pkcs12));
  }

  /// <summary>
  ///   Basic options with pinned certificate
  /// </summary>
  /// <param name="authToken">Authentication token (pre-shared key, password)</param>
  /// <param name="certificate">Certificate to use (either to use in the server, or to expect)</param>
  public PlaneDBRemoteOptions(string authToken, X509Certificate2 certificate)
  {
    AuthToken = authToken;

    Certificate = certificate;
  }

  /// <summary>
  ///   Basic options with pinned certificate
  /// </summary>
  /// <param name="authToken">Authentication token (pre-shared key, password)</param>
  /// <param name="certificate">Certificate to use (either to use in the server, or to expect)</param>
  public PlaneDBRemoteOptions(string authToken, FileInfo? certificate)
  {
    AuthToken = authToken;

    if (certificate != null) {
      Certificate = new X509Certificate2(certificate.FullName);
    }
  }

  /// <summary>
  ///   Address to connect to (or bind, in case of servers)
  /// </summary>
  public IPAddress Address { get; init; } = IPAddress.Loopback;

  /// <summary>
  ///   Port to connect to (or bind to, in case of servers)
  /// </summary>
  public int Port { get; init; }

  /// <inheritdoc />
  public void Dispose()
  {
    Certificate?.Dispose();
  }

  /// <summary>
  ///   Adds a database to serve with a PlaneDB.Remote server.
  /// </summary>
  /// <param name="moniker">Moniker that clients will use to identify the db</param>
  /// <param name="db">DB to serve</param>
  /// <returns>Self</returns>
  public PlaneDBRemoteOptions AddDatabaseToServe(
    string moniker,
    IPlaneDB<byte[], byte[]> db)
  {
    databases.Add(moniker, db);

    return this;
  }

  /// <summary>
  ///   Adds a database to serve with a PlaneDB.Remote server.
  /// </summary>
  /// <param name="moniker">Moniker that clients will use to identify the db</param>
  /// <param name="db">DB to serve</param>
  /// <returns>Self</returns>
  public PlaneDBRemoteOptions AddDatabaseToServe<TKey, TValue>(
    string moniker,
    IPlaneDB<TKey, TValue> db) where TKey : notnull
  {
    databases.Add(moniker, db.BaseDB);

    return this;
  }

  /// <summary>
  ///   Adds a database to serve with a PlaneDB.Remote server.
  /// </summary>
  /// <param name="moniker">Moniker that clients will use to identify the db</param>
  /// <param name="db">DB to serve</param>
  /// <returns>Self</returns>
  public PlaneDBRemoteOptions AddDatabaseToServe<TKey>(string moniker, IPlaneSet<TKey> db)
    where TKey : notnull
  {
    databases.Add(moniker, db.BaseDB);

    return this;
  }

  internal byte[] ComputeSecret(byte[] nonce)
  {
    // The idea here is to compute a one time secret, salted with a nonce, not to enable secure storage of
    // derived secrets.
    return new HMACSHA256(Encoding.UTF8.GetBytes(AuthToken)).ComputeHash(nonce);
  }

  /// <summary>
  ///   Connect to a remote PlaneDB
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>New instance of remote PlaneDB</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneDB<byte[], byte[]> ConnectDB(string moniker, CancellationToken token)
  {
    return new PlaneDBRemoteClient(this, moniker, token);
  }

  /// <summary>
  ///   Connect to a remote typed PlaneDB
  /// </summary>
  /// <param name="keySerializer">Serializer to use for keys</param>
  /// <param name="valueSerializer">Serializer to use for values</param>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>New instance of remote typed PlaneDB</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneDB<TKey, TValue> ConnectDB<TKey, TValue>(
    IPlaneSerializer<TKey> keySerializer,
    IPlaneSerializer<TValue> valueSerializer,
    string moniker,
    CancellationToken token) where TKey : notnull
  {
    return new TypedPlaneDB<TKey, TValue>(
      keySerializer,
      valueSerializer,
      new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote PlaneSet
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>Instance of remote PlaneSet</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneSet<byte[]> ConnectSet(string moniker, CancellationToken token)
  {
    return new PlaneSet(new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote typed PlaneSet
  /// </summary>
  /// <param name="keySerializer">Serializer to use for keys</param>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>Instance of remote typed PlaneSet</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneSet<TKey> ConnectSet<TKey>(
    IPlaneSerializer<TKey> keySerializer,
    string moniker,
    CancellationToken token) where TKey : notnull
  {
    return new TypedPlaneSet<TKey>(
      keySerializer,
      new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote string PlaneDB
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>New instance of remote string PlaneDB</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneDB<string, string> ConnectStringDB(string moniker, CancellationToken token)
  {
    var stringSerializer = new PlaneStringSerializer();

    return new TypedPlaneDB<string, string>(
      stringSerializer,
      stringSerializer,
      new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote string PlaneDB
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="encoding">String encoding</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>New instance of remote typed PlaneDB</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneDB<string, string> ConnectStringDB(
    string moniker,
    Encoding encoding,
    CancellationToken token)
  {
    var stringSerializer = new PlaneStringSerializer(encoding);

    return new TypedPlaneDB<string, string>(
      stringSerializer,
      stringSerializer,
      new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote string PlaneSet
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>Instance of remote string PlaneSet</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneSet<string> ConnectStringSet(string moniker, CancellationToken token)
  {
    return new TypedPlaneSet<string>(
      new PlaneStringSerializer(),
      new PlaneDBRemoteClient(this, moniker, token));
  }

  /// <summary>
  ///   Connect to a remote string PlaneSet
  /// </summary>
  /// <param name="moniker">Database moniker</param>
  /// <param name="encoding">String encoding</param>
  /// <param name="token">Cancellation token to abort communications</param>
  /// <returns>Instance of remote string PlaneSet</returns>
  /// <remarks>Remote instances may not support the full feature set.</remarks>
  public IPlaneSet<string> ConnectStringSet(
    string moniker,
    Encoding encoding,
    CancellationToken token)
  {
    return new TypedPlaneSet<string>(
      new PlaneStringSerializer(encoding),
      new PlaneDBRemoteClient(this, moniker, token));
  }

  internal IPlaneDB<byte[], byte[]> GetDB(string moniker)
  {
    if (!databases.TryGetValue(moniker, out var rv)) {
      throw new IOException($"Not serving a database by the moniker '{moniker}'");
    }

    return rv;
  }
}

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Serve a DB over the network")]
[PublicAPI]
internal sealed class ServeCommand(Options owner) : GetOptCommand<Options>(owner)
{
  [Argument(
    "token",
    HelpText = "Authentication token (pre-shared key, password)",
    Required = true)]
  [ShortArgument('t')]
  public string AuthToken = string.Empty;

  [Argument(
    "address",
    HelpText = "Local bind address (all and local, or IPs are valid options)")]
  [ShortArgument('h')]
  public string BindAddress = string.Empty;

  [Argument("Certificate", HelpText = "Certificate to use (enables TLS)")]
  [ShortArgument('c')]
  public FileInfo? Certificate;

  [Parameters(Exact = 1, HelpVar = "DB")]
  public DirectoryInfo[] DB = [];

  [Argument("no-journal", HelpText = "Disable journal")]
  [FlagArgument(true)]
  public bool NoJournal;

  [Argument("port", HelpText = "Port to use")]
  [ShortArgument('p')]
  public ushort Port;

  [Argument("redis", HelpText = "Serve as Redis-Server")]
  [ShortArgument('r')]
  [FlagArgument(true)]
  public bool Redis;

  [Argument("self-signed", HelpText = "Generate a self-signed certificate (enabled TLS)")]
  [FlagArgument(true)]
  public bool SelfSigned;

  public override string Name => "serve";

  public override void Execute()
  {
    var address = IPAddress.Any;
    if (!string.IsNullOrEmpty(BindAddress)) {
      address = BindAddress.ToLowerInvariant() switch {
        "all" => IPAddress.Any,
        "local" => IPAddress.Loopback,
        _ => IPAddress.Parse(BindAddress)
      };
    }

    var remoteOptions = Certificate switch {
      null => new PlaneDBRemoteOptions(AuthToken, SelfSigned) {
        Port = Port,
        Address = address
      },
      _ => new PlaneDBRemoteOptions(AuthToken, Certificate) {
        Port = Port,
        Address = address
      }
    };

    var planeOpts = new PlaneOptions();
    if (NoJournal) {
      planeOpts = planeOpts.DisableJournal();
    }

    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    planeOpts = planeOpts.WithOpenMode(
      Owner.Packed ? PlaneOpenMode.Packed : PlaneOpenMode.ReadWrite);

    if (DB is not { Length: 1 }) {
      throw new GetOptException("No database specified");
    }

    var db = DB[0];

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      planeOpts = planeOpts.UsingTablespace(Owner.Tablespace);
    }

    using var plane = new PlaneDB(db, planeOpts);

    var ct = new CancellationTokenSource();
    Console.CancelKeyPress += (_, args) => {
      ct.Cancel();
      args.Cancel = true;
    };

    try {
      var myWriter = new TextWriterTraceListener(Console.Out);
      _ = Trace.Listeners.Add(myWriter);
      using var remote = Redis switch {
        true => plane.ServeRedis(remoteOptions, ct.Token),
        _ => remoteOptions.AddDatabaseToServe("default", plane).Serve(ct.Token)
      };
      var mode = Redis ? "redis" : "binary";
      var tls = remoteOptions.Certificate switch {
        null => "not encrypted",
        _ =>
          $"{remoteOptions.Certificate.SubjectName.Name} - {remoteOptions.Certificate.Thumbprint}"
      };
      Console.WriteLine($"Serving as {mode} on {remote.Address}:{remote.Port} ({tls})");
      remote.Wait();
    }
    catch (OperationCanceledException) {
      // ignored;
    }
  }
}

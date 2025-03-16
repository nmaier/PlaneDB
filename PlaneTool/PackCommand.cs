using System.IO;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Pack a database")]
[PublicAPI]
internal sealed class PackCommand(Options owner) : GetOptCommand<Options>(owner)
{
  [Argument("out-compressed", HelpText = "Create compressed pack")]
  [ShortArgument('c')]
  [FlagArgument(true)]
  public bool Compressed;

  [Parameters(Exact = 1, HelpVar = "DB")]
  public DirectoryInfo[]? DB;

  [Argument(HelpText = "Output the packed db to this file")]
  [ShortArgument('o')]
  public FileInfo? Out;

  [Argument(
    "out-passphrase",
    HelpText = "Encrypt pack with this pass-phrase; implies compression")]
  [ShortArgument('p')]
  public string? Passphrase;

  public override string Name => "pack";

  public override void Execute()
  {
    var planeOpts = new PlaneOptions().DisableJournal().DisableThreadSafety();
    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    planeOpts = planeOpts.WithOpenMode(
      Owner.Packed ? PlaneOpenMode.Packed : PlaneOpenMode.ReadOnly);

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      planeOpts = planeOpts.UsingTablespace(Owner.Tablespace);
    }

    if (DB is not { Length: 1 }) {
      throw new GetOptException("No database specified");
    }

    if (Out == null) {
      throw new GetOptException("No Out specified");
    }

    var outOpts = new PlaneOptions();
    if (!string.IsNullOrEmpty(Passphrase)) {
      outOpts = outOpts.WithEncryption(Passphrase);
    }
    else if (Compressed) {
      outOpts = outOpts.WithCompression();
    }

    using var plane = new PlaneDB(DB[0], planeOpts);
    plane.WriteToPack(Out, outOpts);
  }
}

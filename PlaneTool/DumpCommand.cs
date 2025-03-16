using System;
using System.Buffers.Binary;
using System.IO;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Dump a database to a compact binary file")]
[PublicAPI]
internal sealed class DumpCommand(Options owner) : GetOptCommand<Options>(owner)
{
  [Parameters(Exact = 1, HelpVar = "DB")]
  public DirectoryInfo[]? DB;

  [Argument(Required = true, HelpVar = "DumpFile")]
  public FileInfo? Out;

  public override string Name => "dump";

  public override void Execute()
  {
    var planeOpts = new PlaneOptions().DisableJournal();
    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      planeOpts = planeOpts.UsingTablespace(Owner.Tablespace);
    }

    planeOpts = planeOpts.WithOpenMode(
      Owner.Packed ? PlaneOpenMode.Packed : PlaneOpenMode.ReadOnly);

    if (DB is not { Length: 1 }) {
      throw new GetOptException("No database specified");
    }

    if (Out == null) {
      throw new GetOptException("No Out specified");
    }

    using var dump = new FileStream(
      Out.FullName,
      FileMode.Create,
      FileAccess.Write,
      FileShare.None,
      16 * 1024,
      FileOptions.SequentialScan);
    dump.Write("PDBD"u8);
    using var plane = new PlaneDB(DB[0], planeOpts);
    Span<byte> lengths = stackalloc byte[sizeof(int) * 2];
    var copyCount = 0;
    foreach (var (key, value) in plane) {
      BinaryPrimitives.WriteInt32LittleEndian(lengths, key.Length);
      BinaryPrimitives.WriteInt32LittleEndian(lengths[sizeof(int)..], value.Length);
      dump.Write(lengths);
      if (key.Length > 0) {
        dump.Write(key);
      }

      if (value.Length > 0) {
        dump.Write(value);
      }

      copyCount++;
      if (copyCount > 0 && copyCount % 10_000 == 0) {
        Console.WriteLine($"{copyCount:N0} entries copied...");
      }
    }

    Console.WriteLine($"{copyCount:N0} entries copied in total");
  }
}

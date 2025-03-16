using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;

using JetBrains.Annotations;

using NMaier.BlockStream;
using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Restore a dumped db to a PlaneDB tablespace",
  UsageEpilog = "Warning: The tablespace will be cleared!")]
[PublicAPI]
internal sealed class RestoreCommand(Options owner) : GetOptCommand<Options>(owner)
{
  private static int copyCount;

  private static IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateDump(Stream stream)
  {
    var header = new byte[sizeof(int) * 2];
    stream.ReadFullBlock(header, 4);
    if (!header.AsSpan(0, 4).SequenceEqual("PDBD"u8)) {
      throw new IOException("Not a dump file");
    }

    while (stream.Position != stream.Length) {
      stream.ReadFullBlock(header);
      var keyLen = BinaryPrimitives.ReadInt32LittleEndian(header);
      var valLen = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(sizeof(int)));
      byte[] key;
      if (keyLen <= 0) {
        key = [];
      }
      else {
        key = new byte[keyLen];
        stream.ReadFullBlock(key);
      }

      byte[] value;
      if (valLen <= 0) {
        value = [];
      }
      else {
        value = new byte[valLen];
        stream.ReadFullBlock(value);
      }

      yield return new KeyValuePair<byte[], byte[]>(key, value);

      copyCount++;
      if (copyCount > 0 && copyCount % 10_000 == 0) {
        Console.WriteLine($"{copyCount:N0} entries copied...");
      }
    }
  }

  [Argument(HelpVar = "DumpFile", HelpText = "Dump location", Required = true)]
  public FileInfo? From;

  [Argument(HelpVar = "PlaneDB", HelpText = "PlaneDB location", Required = true)]
  public DirectoryInfo? To;

  public override string Name => "restore";

  public override void Execute()
  {
    if (Owner.Packed) {
      throw new GetOptException("Packed mode not allowed in <restore>");
    }

    var planeOpts = new PlaneOptions().DisableJournal().DisableThreadSafety();
    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      planeOpts = planeOpts.UsingTablespace(Owner.Tablespace);
    }

    if (From == null) {
      throw new GetOptException("No from");
    }

    if (!From.Exists) {
      throw new GetOptException("Dump file does not exist");
    }

    if (To == null) {
      throw new GetOptException("No to");
    }

    using var plane = new PlaneDB(To, planeOpts);
    plane.OnFlushMemoryTable += (_, _) => Console.WriteLine("Flushed memory table");
    plane.OnMergedTables += (_, _) => Console.WriteLine("Merged tables");
    plane.Clear();

    using var fs = new FileStream(
      From.FullName,
      FileMode.Open,
      FileAccess.Read,
      FileShare.Read,
      16384,
      FileOptions.SequentialScan);

    var (added, ignored) = plane.TryAdd(EnumerateDump(fs));
    Console.WriteLine(
      ignored > 0
        ? $"{added:N0} entries copied in total, {ignored:N0} ignored (duplicate keys)"
        : $"{added:N0} entries copied in total");
  }
}

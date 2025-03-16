using System;
using System.Collections.Generic;
using System.IO;

using JetBrains.Annotations;

using NMaier.GetOptNet;

using RocksDbSharp;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Import a RocksDB or LevelDB to a PlaneDB tablespace",
  UsageEpilog = "Warning: The tablespace will be cleared!")]
[PublicAPI]
internal sealed class ImportRocksDBCommand(Options owner) : GetOptCommand<Options>(owner)
{
  private static int copyCount;

  private static IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateRocks(Iterator iter)
  {
    for (_ = iter.SeekToFirst(); iter.Valid(); _ = iter.Next()) {
      yield return new KeyValuePair<byte[], byte[]>(iter.Key(), iter.Value());
      copyCount++;
      if (copyCount > 0 && copyCount % 10_000 == 0) {
        Console.WriteLine($"{copyCount:N0} entries copied...");
      }
    }
  }

  [Argument(HelpVar = "RockDB", HelpText = "RocksDB location", Required = true)]
  public DirectoryInfo? From;

  [Argument(HelpVar = "PlaneDB", HelpText = "PlaneDB location", Required = true)]
  public DirectoryInfo? To;

  public override string Name => "importrocksdb";

  public override void Execute()
  {
    if (Owner.Packed) {
      throw new GetOptException("Packed mode not allowed in <import>");
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

    if (To == null) {
      throw new GetOptException("No to");
    }

    using var rocks = RocksDb.OpenReadOnly(new DbOptions(), From.FullName, false);
    using var plane = new PlaneDB(To, planeOpts);
    plane.OnFlushMemoryTable += (_, _) => Console.WriteLine("Flushed memory table");
    plane.OnMergedTables += (_, _) => Console.WriteLine("Merged tables");
    plane.Clear();

    var iter = rocks.NewIterator();

    var (added, ignored) = plane.TryAdd(EnumerateRocks(iter));
    Console.WriteLine(
      ignored > 0
        ? $"{added:N0} entries copied in total, {ignored:N0} ignored (duplicate keys)"
        : $"{added:N0} entries copied in total");
  }
}

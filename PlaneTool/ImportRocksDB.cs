using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using NMaier.GetOptNet;
using NMaier.PlaneDB;
using RocksDbSharp;

#pragma warning disable 649

namespace PlaneTool
{
  [GetOptOptions(AcceptPrefixType = ArgumentPrefixTypes.Dashes, OnUnknownArgument = UnknownArgumentsAction.Throw,
    UsageIntro = "Import a RocksDB or LevelDB to a PlaneDB tablespace",
    UsageEpilog = "Warning: The tablespace will be cleared!")]
  [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
  internal sealed class ImportRocksDB : GetOptCommand<Options>
  {
    private static int copyCount;

    private static void CopyFromRocks(PlaneDB plane, Iterator iter)
    {
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        plane.Set(iter.Key(), iter.Value());
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

    public ImportRocksDB(Options owner) : base(owner)
    {
    }

    public override string Name => "import";

    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public override void Execute()
    {
      var popts = new PlaneDBOptions().DisableJournal();
      if (!string.IsNullOrEmpty(Owner.Passphrase)) {
        popts = popts.EnableEncryption(Owner.Passphrase);
      }
      else if (Owner.Compressed) {
        popts = popts.EnableCompression();
      }

      if (!string.IsNullOrEmpty(Owner.Tablespace)) {
        popts = popts.UsingTableSpace(Owner.Tablespace);
      }

      if (From == null) {
        throw new GetOptException("No from");
      }

      if (To == null) {
        throw new GetOptException("No to");
      }

      using var rocks = RocksDb.OpenReadOnly(new DbOptions(), From.FullName, false);
      using var plane = new PlaneDB(To, FileMode.OpenOrCreate, popts);
      plane.OnFlushMemoryTable += (_, __) => Console.WriteLine("Flushed memory table");
      plane.OnMergedTables += (_, __) => Console.WriteLine("Merged tables");
      plane.Clear();

      var iter = rocks.NewIterator();

      plane.MassInsert(() => CopyFromRocks(plane, iter));

      Console.WriteLine($"{copyCount:N0} entries copied in total");
    }
  }
}
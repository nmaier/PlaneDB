using System.Diagnostics.CodeAnalysis;
using System.IO;
using NMaier.GetOptNet;
using NMaier.PlaneDB;

#pragma warning disable 649

namespace PlaneTool
{
  [GetOptOptions(AcceptPrefixType = ArgumentPrefixTypes.Dashes, OnUnknownArgument = UnknownArgumentsAction.Throw,
                 UsageIntro = "Compacts a database")]
  [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
  internal sealed class Compact : GetOptCommand<Options>
  {
    private static void CompactOne(DirectoryInfo db, PlaneDBOptions popts)
    {
      var id = $"{db.FullName}:{popts.TableSpace}";
      try {
        Options.Write($"{id} - Opening");
        using var plane = new PlaneDB(db, FileMode.Open, popts);
        plane.OnFlushMemoryTable += (_, __) => Options.Error($"{id} - Flushed memory table");
        plane.OnMergedTables += (_, __) => Options.Error($"{id} - Merged tables");
        Options.Write($"{id} - Starting compaction");
        plane.Compact();
        Options.Write($"{id} - Finished compaction");
      }
      catch (BadMagicException ex) {
        Options.Error($"{id} - {ex.Message}");
      }
      catch (AlreadyLockedException ex) {
        Options.Error($"{id} - {ex.Message}");
      }
    }

    [Argument(HelpText = "Apply to all tablespaces")]
    [ShortArgument('a')]
    [FlagArgument(true)]
    public bool AllTablespaces;

    [Parameters(Exact = 1, HelpVar = "DB")]
    public DirectoryInfo[]? DB;

    public Compact(Options owner) : base(owner)
    {
    }

    public override string Name => "compact";

    public override void Execute()
    {
      var popts = new PlaneDBOptions();
      if (!string.IsNullOrEmpty(Owner.Passphrase)) {
        popts = popts.EnableEncryption(Owner.Passphrase);
      }
      else if (Owner.Compressed) {
        popts = popts.EnableCompression();
      }

      if (DB == null || DB.Length != 1) {
        throw new GetOptException("No database specified");
      }

      var db = DB[0];

      if (!string.IsNullOrEmpty(Owner.Tablespace)) {
        CompactOne(db, popts.UsingTableSpace(Owner.Tablespace));
      }
      else if (AllTablespaces) {
        foreach (string ts in Options.GetTableSpaces(db)) {
          CompactOne(db, popts.UsingTableSpace(ts));
        }
      }
      else {
        CompactOne(db, popts);
      }
    }
  }
}
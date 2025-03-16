using System.IO;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Compacts a database")]
[PublicAPI]
internal sealed class CompactCommand(Options owner) : GetOptCommand<Options>(owner)
{
  private static void CompactOne(DirectoryInfo db, PlaneOptions planeOpts, bool fully)
  {
    var id = $"{db.FullName}:{planeOpts.Tablespace}";
    try {
      Options.Write($"{id} - Opening");
      using var plane = new PlaneDB(db, planeOpts);
      plane.OnFlushMemoryTable +=
        (_, _) => Options.WriteError($"{id} - Flushed memory table");
      plane.OnMergedTables += (_, _) => Options.WriteError($"{id} - Merged tables");
      Options.Write($"{id} - Starting compaction");
      plane.Compact(fully ? CompactionMode.Fully : CompactionMode.Normal);
      Options.Write($"{id} - Finished compaction");
    }
    catch (PlaneDBBadMagicException ex) {
      Options.WriteError($"{id} - {ex.Message}");
    }
    catch (PlaneDBAlreadyLockedException ex) {
      Options.WriteError($"{id} - {ex.Message}");
    }
  }

  [Argument(HelpText = "Apply to all table spaces")]
  [ShortArgument('a')]
  [FlagArgument(true)]
  public bool AllTablespaces;

  [Parameters(Exact = 1, HelpVar = "DB")]
  public DirectoryInfo[]? DB;

  [Argument(HelpText = "Compact fully")]
  [ShortArgument('f')]
  [FlagArgument(true)]
  public bool Fully;

  [Argument(HelpText = "Try to repair db (backup first)")]
  [ShortArgument('r')]
  [FlagArgument(true)]
  public bool Repair;

  public override string Name => "compact";

  public override void Execute()
  {
    if (Owner.Packed) {
      throw new GetOptException("Packed mode not allowed in <compact>");
    }

    var planeOpts = new PlaneOptions();
    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    if (Repair) {
      planeOpts = planeOpts.ActivateRepairMode(
        (@base, args) => Options.WriteError(
          $"{@base.Location.FullName}:{@base.TableSpace}: Repaired {args.File.Name}: {args.Reason}"));
    }

    if (DB is not { Length: 1 }) {
      throw new GetOptException("No database specified");
    }

    var db = DB[0];

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      CompactOne(db, planeOpts.UsingTablespace(Owner.Tablespace), Fully);
    }
    else if (AllTablespaces) {
      foreach (var ts in Options.GetTableSpaces(db)) {
        CompactOne(db, planeOpts.UsingTablespace(ts), Fully);
      }
    }
    else {
      CompactOne(db, planeOpts, Fully);
    }
  }
}

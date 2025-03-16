using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Print information about a DB")]
[PublicAPI]
internal sealed class InfoCommand(Options owner) : GetOptCommand<Options>(owner)
{
  private const int INLINED_SIZE = 9;
  private readonly List<KeyValuePair<string, string>> infos = [];

  [Argument("all", HelpText = "Apply to all table spaces")]
  [ShortArgument('a')]
  [FlagArgument(true)]
  public bool AllTablespaces;

  [Parameters(Exact = 1, HelpVar = "DB")]
  public DirectoryInfo[] DB = [];

  [Argument]
  [ShortArgument('v')]
  [CountedArgument]
  public int Verbose;

  public override string Name => "info";

  public override void Execute()
  {
    if (Owner.Packed && AllTablespaces) {
      throw new GetOptException("Cannot use --packed and --all at the same time");
    }

    var planeOpts = new PlaneOptions().DisableJournal().WithBlockCacheCapacity(2_048);
    if (!string.IsNullOrEmpty(Owner.Passphrase)) {
      planeOpts = planeOpts.WithEncryption(Owner.Passphrase);
    }
    else if (Owner.Compressed) {
      planeOpts = planeOpts.WithCompression();
    }

    planeOpts = planeOpts.WithOpenMode(
      Owner.Packed ? PlaneOpenMode.Packed : PlaneOpenMode.ReadOnly);

    if (DB is not { Length: 1 }) {
      throw new GetOptException("No database specified");
    }

    var db = DB[0];

    if (!string.IsNullOrEmpty(Owner.Tablespace)) {
      InfoOne(db, planeOpts.UsingTablespace(Owner.Tablespace));
    }
    else if (AllTablespaces) {
      foreach (var tableSpace in Options.GetTableSpaces(db)) {
        InfoOne(db, planeOpts.UsingTablespace(tableSpace));
      }
    }
    else {
      InfoOne(db, planeOpts);
    }
  }

  private void Add(string section)
  {
    infos.Add(new KeyValuePair<string, string>(section, string.Empty));
  }

  private void Add(string entry, string value)
  {
    infos.Add(new KeyValuePair<string, string>(entry, value));
  }

  private void Add(string entry, long value)
  {
    infos.Add(new KeyValuePair<string, string>(entry, $"{value:N0}"));
  }

  private void Add(string entry, double value)
  {
    infos.Add(new KeyValuePair<string, string>(entry, $"{value:N3}"));
  }

  private void AddByte(string entry, long absValue)
  {
    var value = Math.Abs(absValue);
    if (value < 1024) {
      infos.Add(new KeyValuePair<string, string>(entry, $"{absValue:N0} B"));

      return;
    }

    var neg = absValue < 0;
    var fmt = value / 1024.0;
    var unit = "KiB";

    if (fmt >= 1024) {
      fmt /= 1024;
      unit = "MiB";
    }

    if (fmt >= 1024) {
      fmt /= 1024;
      unit = "GiB";
    }

    if (fmt >= 1024) {
      fmt /= 1024;
      unit = "TiB";
    }

    if (neg) {
      fmt = -fmt;
    }

    infos.Add(
      new KeyValuePair<string, string>(entry, $"{absValue:N0} B ({fmt:N2} {unit})"));
  }

  private void InfoOne(DirectoryInfo db, PlaneOptions planeOpts)
  {
    infos.Clear();
    try {
      var stop = new Stopwatch();
      stop.Start();
      Options.WriteError($"{db.FullName}:{planeOpts.Tablespace} - Querying");
      using var plane = new PlaneDB(db, planeOpts);
      stop.Stop();
      Add("General Information");
      Add("Time To Open", stop.Elapsed.ToString());
      Add("Location", plane.Location.FullName);

      if (!string.IsNullOrEmpty(plane.TableSpace)) {
        Add("Tablespace", plane.TableSpace);
      }

      if (Verbose > 0) {
        Add("Table Files", plane.CurrentTableCount);
        Add("Index Blocks", plane.CurrentIndexBlockCount);
        Add("Sequence", string.Join(", ", plane.TableSequence));

        foreach (var (key, value) in plane.AllLevels) {
          Add($"Level {key}", string.Join(", ", value));
        }
      }

      var currentDiskSize = plane.CurrentDiskSize;
      var currentRealSize = plane.CurrentRealSize;

      Add("Sizes");
      AddByte("Disk Size", currentDiskSize);
      AddByte("Raw Size", currentRealSize);
      if (Verbose > 0) {
        Add("Bloom Bits", plane.CurrentBloomBits);
        AddByte(
          "Bloom Bytes (In Memory)",
          (long)Math.Ceiling(plane.CurrentBloomBits / 8.0));
      }

      stop.Reset();
      stop.Start();

      Add("Item Information");
      var count = 0;
      var minK = int.MaxValue;
      var maxK = 0;
      var minV = int.MaxValue;
      var maxV = 0;
      long allK = 0;
      long allV = 0;
      long inlined = 0;
      if (Verbose <= 1) {
        count = plane.Count;
      }
      else {
        foreach (var (key, value) in plane) {
          ++count;
          var kl = key.Length;
          var vl = value.Length;
          if (vl <= INLINED_SIZE) {
            ++inlined;
          }

          minK = Math.Min(kl, minK);
          minV = Math.Min(vl, minV);
          maxK = Math.Max(kl, maxK);
          maxV = Math.Max(vl, maxV);
          allK += kl;
          allV += vl;
        }
      }

      stop.Stop();

      Add("Time To Gather Information", stop.Elapsed.ToString());
      Add("Item count", count);
      if (Verbose > 1 && count > 0) {
        if (minK != maxK) {
          AddByte("Min Key Length", minK);
          AddByte("Max Key Length", maxK);
          Add("Average Key Length", allK / (double)count);
        }
        else {
          AddByte("Key Length", minK);
        }

        if (minV != maxV) {
          AddByte("Min Value Length", minV);
          AddByte("Max Value Length", maxV);
          Add("Average Value Length", allV / (double)count);
        }
        else {
          AddByte("Value Length", minV);
        }

        AddByte("Keys Length", allK);
        AddByte("Values Length", allV);
        Add("Inlined Values", inlined);
      }

      if (Verbose > 1) {
        Add("Overheads (bookkeeping, bloom, etc)");
        var dataLen = allV + allK;
        AddByte("Raw Data Length", dataLen);
        AddByte("Disk Overhead", currentDiskSize - dataLen);
        AddByte("Raw Overhead", currentRealSize - dataLen);
        Add("Disk factor", (double)currentDiskSize / dataLen);
        Add("Raw factor", (double)currentRealSize / dataLen);
      }

      Options.Write($"{db.FullName}:{planeOpts.Tablespace} - Information");
      var maxLen =
        infos.Where(i => !string.IsNullOrEmpty(i.Value)).Max(i => i.Key.Length) + 4;
      var maxValLen = infos.Max(i => i.Value.Length) + 2;
      foreach (var (key, value) in infos) {
        if (string.IsNullOrEmpty(value)) {
          var termLen = Math.Min(Console.WindowWidth - 2, maxLen + maxValLen);
          var half = (int)Math.Ceiling((termLen - key.Length) / 2.0);
          Console.WriteLine();
          Console.WriteLine(
            $"{key.PadLeft(key.Length + 1).PadRight(key.Length + 2).PadLeft(half + key.Length, '-').PadRight(termLen, '-')}");
          Console.WriteLine();

          continue;
        }

        Console.WriteLine($"{key.PadLeft(maxLen)}: {value}");
      }

      Console.WriteLine();
    }
    catch (PlaneDBBadMagicException ex) {
      Options.WriteError($"{db.FullName}:{planeOpts.Tablespace} - {ex.Message}");
    }
    catch (PlaneDBAlreadyLockedException ex) {
      Options.WriteError($"{db.FullName}:{planeOpts.Tablespace} - {ex.Message}");
    }
  }
}

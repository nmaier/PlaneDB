using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using NMaier.GetOptNet;
using NMaier.PlaneDB;

#pragma warning disable 649

namespace PlaneTool
{
  [GetOptOptions(AcceptPrefixType = ArgumentPrefixTypes.Dashes, OnUnknownArgument = UnknownArgumentsAction.Throw,
    UsageIntro = "Print information about a DB")]
  [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
  [SuppressMessage("ReSharper", "ConvertToConstant.Global")]
  internal sealed class Info : GetOptCommand<Options>
  {
    private readonly List<KeyValuePair<string, string>> infos = new List<KeyValuePair<string, string>>();

    [Argument(HelpText = "Apply to all tablespaces")]
    [ShortArgument('a')]
    [FlagArgument(true)]
    public bool AllTablespaces;

    [Parameters(Exact = 1, HelpVar = "DB")]
    [SuppressMessage("ReSharper", "FieldCanBeMadeReadOnly.Global")]
    public DirectoryInfo[] DB = Array.Empty<DirectoryInfo>();

    [Argument]
    [ShortArgument('v')]
    [FlagArgument(true)]
    [SuppressMessage("ReSharper", "FieldCanBeMadeReadOnly.Global")]
    public bool Verbose = false;

    public Info(Options owner) : base(owner)
    {
    }

    public override string Name => "info";


    public override void Execute()
    {
      var popts = new PlaneDBOptions().DisableJournal().WithBlockCacheCapacity(2_048);
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
        InfoOne(db, popts.UsingTableSpace(Owner.Tablespace));
      }
      else if (AllTablespaces) {
        foreach (var tableSpace in Options.GetTableSpaces(db)) {
          InfoOne(db, popts.UsingTableSpace(tableSpace));
        }
      }
      else {
        InfoOne(db, popts);
      }
    }

    void Add(string section)
    {
      infos.Add(new KeyValuePair<string, string>(section, string.Empty));
    }

    void Add(string entry, string value)
    {
      infos.Add(new KeyValuePair<string, string>(entry, value));
    }

    void Add(string entry, long value)
    {
      infos.Add(new KeyValuePair<string, string>(entry, $"{value:N0}"));
    }

    void Add(string entry, double value)
    {
      infos.Add(new KeyValuePair<string, string>(entry, $"{value:N3}"));
    }

    void AddByte(string entry, long avalue)
    {
      var value = Math.Abs(avalue);
      if (value < 1024) {
        infos.Add(new KeyValuePair<string, string>(entry, $"{avalue:N0} B"));
        return;
      }

      var neg = avalue < 0;
      double fmt = value / 1024.0;
      string unit = "KiB";

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

      infos.Add(new KeyValuePair<string, string>(entry, $"{avalue:N0} B ({fmt:N2} {unit})"));
    }

    private void InfoOne(DirectoryInfo db, PlaneDBOptions popts)
    {
      infos.Clear();
      try {
        var stop = new Stopwatch();
        stop.Start();
        Options.Write($"{db.FullName}:{popts.TableSpace} - Querying");
        using var plane = new PlaneDB(db, FileMode.Open, popts);
        stop.Stop();
        Add("General Information");
        Add("Time To Open", stop.Elapsed.ToString());
        Add("Location", plane.Location.FullName);
        Add("Tablespace", plane.TableSpace);
        if (Verbose) {
          Add("Table Files", plane.CurrentTableCount);
          Add("Index Blocks", plane.CurrentIndexBlockCount);
        }

        var currentDiskSize = plane.CurrentDiskSize;
        var currentRealSize = plane.CurrentRealSize;

        Add("Sizes");
        AddByte("Disk Size", currentDiskSize);
        AddByte("Raw Size", currentRealSize);
        if (Verbose) {
          Add("Bloom Bits", plane.CurrentBloomBits);
          AddByte("Bloom Bytes (In Memory)", (long)Math.Ceiling(plane.CurrentBloomBits / 8.0));
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
        if (!Verbose) {
          count = plane.Count;
        }
        else {
          foreach (var (key, value) in plane) {
            ++count;
            var kl = key.Length;
            var vl = value.Length;
            if (vl <= 9) {
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
        if (Verbose && count > 0) {
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

        if (Verbose) {
          Add("Overheads (bookkeeping, bloom, etc)");
          var datalen = allV + allK;
          AddByte("Raw Data Length", datalen);
          AddByte("Disk Overhead", currentDiskSize - datalen);
          AddByte("Raw Overhead", currentRealSize - datalen);
          Add("Disk factor", (double)currentDiskSize / datalen);
          Add("Raw factor", (double)currentRealSize / datalen);
        }


        var maxlen = infos.Where(i => !string.IsNullOrEmpty(i.Value)).Max(i => i.Key.Length) + 4;
        var maxvlen = infos.Max(i => i.Value.Length) + 2;
        foreach (var (key, value) in infos) {
          if (string.IsNullOrEmpty(value)) {
            var tlen = Math.Min(Console.WindowWidth - 2, maxlen + maxvlen);
            var half = (int)Math.Ceiling((tlen - key.Length) / 2.0);
            Console.WriteLine();
            Console.WriteLine(
              $"{key.PadLeft(key.Length + 1).PadRight(key.Length + 2).PadLeft(half + key.Length, '-').PadRight(tlen, '-')}");
            Console.WriteLine();
            continue;
          }

          Console.WriteLine($"{key.PadLeft(maxlen)}: {value}");
        }

        Console.WriteLine();
      }
      catch (BadMagicException ex) {
        Options.Error($"{db.FullName}:{popts.TableSpace} - {ex.Message}");
      }
      catch (AlreadyLockedException ex) {
        Options.Error($"{db.FullName}:{popts.TableSpace} - {ex.Message}");
      }
    }
  }
}
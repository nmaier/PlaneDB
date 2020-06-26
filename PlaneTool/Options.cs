using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NMaier.GetOptNet;

#pragma warning disable 649

namespace PlaneTool
{
  [GetOptOptions(AcceptPrefixType = ArgumentPrefixTypes.Dashes, OnUnknownArgument = UnknownArgumentsAction.Throw,
                 UsageIntro = "Various tools for managing a PlaneDB")]
  internal sealed class Options : GetOpt
  {
    public static void Error(string msg)
    {
      lock (Console.Error) {
        Console.Error.WriteLine($"[{DateTime.UtcNow}] - {msg}");
      }
    }

    public static void Write(string msg)
    {
      lock (Console.Out) {
        Console.Out.WriteLine($"[{DateTime.UtcNow}] - {msg}");
      }
    }

    internal static IEnumerable<string> GetTableSpaces(DirectoryInfo db)
    {
      return db.GetFiles("*-MANIFEST.planedb").Select(fileInfo => fileInfo.Name.Replace("-MANIFEST.planedb", ""))
        .OrderBy(i => i, StringComparer.Ordinal);
    }

    [Argument(HelpText = "Compressed database")]
    [FlagArgument(true)]
    public bool Compressed;

    [Argument(HelpText = "Encrypt with this passphrase; implies compression")]
    public string? Passphrase;

    [Argument(HelpText = "PlaneDB Tablespace")]
    public string? Tablespace;

    internal Options()
    {
      AddCommand(new ImportRocksDB(this));
      AddCommand(new Compact(this));
      AddCommand(new Info(this));
      AddCommand(new Dump(this));
      AddHelpCommand(true);
    }
  }
}
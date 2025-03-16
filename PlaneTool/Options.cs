using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using JetBrains.Annotations;

using NMaier.GetOptNet;

#pragma warning disable 649

namespace NMaier.PlaneDB;

[GetOptOptions(
  AcceptPrefixType = ArgumentPrefixTypes.Dashes,
  OnUnknownArgument = UnknownArgumentsAction.Throw,
  UsageIntro = "Various tools for managing a PlaneDB")]
[PublicAPI]
internal sealed class Options : GetOpt
{
  internal static IEnumerable<string> GetTableSpaces(DirectoryInfo db)
  {
    return db.GetFiles("*-MANIFEST.planedb")
      .Select(fileInfo => fileInfo.Name.Replace("-MANIFEST.planedb", ""))
      .OrderBy(i => i, StringComparer.Ordinal);
  }

  public static void Write(string msg)
  {
    lock (Console.Out) {
      Console.Out.WriteLine($"[{DateTime.UtcNow}] - {msg}");
    }
  }

  public static void WriteError(string msg)
  {
    lock (Console.Error) {
      Console.Error.WriteLine($"[{DateTime.UtcNow}] - {msg}");
    }
  }

  [Argument(HelpText = "Compressed database")]
  [FlagArgument(true)]
  public bool Compressed;

  [Argument(HelpText = "Open using packed mode")]
  [ShortArgument('p')]
  public bool Packed;

  [Argument(HelpText = "Encrypt with this pass-phrase; implies compression")]
  public string? Passphrase;

  [Argument(HelpText = "PlaneDB Tablespace")]
  public string? Tablespace;

  internal Options()
  {
    AddCommand(new ImportRocksDBCommand(this));
    AddCommand(new RestoreCommand(this));
    AddCommand(new CompactCommand(this));
    AddCommand(new InfoCommand(this));
    AddCommand(new PackCommand(this));
    AddCommand(new DumpCommand(this));
    AddCommand(new ServeCommand(this));
    AddHelpCommand(true);
  }
}

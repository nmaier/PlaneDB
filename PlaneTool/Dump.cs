using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using NMaier.GetOptNet;
using NMaier.PlaneDB;

#pragma warning disable 649

namespace PlaneTool
{
  [GetOptOptions(AcceptPrefixType = ArgumentPrefixTypes.Dashes, OnUnknownArgument = UnknownArgumentsAction.Throw,
                 UsageIntro = "Dump a database to a compact binary file")]
  [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
  internal sealed class Dump : GetOptCommand<Options>
  {
    [Parameters(Exact = 1, HelpVar = "DB")]
    public DirectoryInfo[]? DB;

    [Argument(Required = true, HelpVar = "DumpFile")]
    public FileInfo? Out;

    public Dump(Options owner) : base(owner)
    {
    }

    public override string Name => "dump";

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

      if (DB == null || DB.Length != 1) {
        throw new GetOptException("No database specified");
      }

      if (Out == null) {
        throw new GetOptException("No Out specified");
      }

      using var dump = new FileStream(Out.FullName, FileMode.Create, FileAccess.Write, FileShare.None, 16 * 1024,
                                      FileOptions.SequentialScan);
      using var plane = new PlaneDB(DB[0], FileMode.Open, popts);
      Span<byte> lengths = stackalloc byte[sizeof(int) * 2];
      foreach (var (key, value) in plane) {
        BinaryPrimitives.WriteInt32LittleEndian(lengths, key.Length);
        BinaryPrimitives.WriteInt32LittleEndian(lengths.Slice(sizeof(int)), value.Length);
        dump.Write(lengths);
        dump.Write(key);
        dump.Write(value);
      }
    }
  }
}
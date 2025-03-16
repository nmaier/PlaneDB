using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using JetBrains.Profiler.Api;

using NMaier.PlaneDB;

var di = new DirectoryInfo("testdb");
if (di.Exists) {
  di.Delete(true);
}

MemoryProfiler.CollectAllocations(true);
var planeOpts = new PlaneOptions().WithEncryption("this is just a test");
FillDB(di, planeOpts);
MemoryProfiler.GetSnapshot();
VerifyDB(di, planeOpts);
MemoryProfiler.GetSnapshot();

return;

void VerifyDB(DirectoryInfo dbDir, PlaneOptions tableOptions)
{
  using var db = new PlaneDB(
    dbDir,
    tableOptions.WithOpenMode(PlaneOpenMode.ExistingReadWrite));
  for (var i = 0; i < 10000; ++i) {
    if (!db.TryGetValue(BitConverter.GetBytes(i), out _)) {
      throw new KeyNotFoundException();
    }
  }
}

static void FillDB(DirectoryInfo dbDir, PlaneOptions tableOptions)
{
  using var db = new PlaneDB(
    dbDir,
    tableOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
  var value = Enumerable.Repeat("hello world"u8.ToArray(), 512)
    .SelectMany(i => i)
    .ToArray();
  Console.WriteLine($"Writing len={value.Length:N0}");

  for (var i = 0; i < 10000; ++i) {
    if (!db.TryAdd(BitConverter.GetBytes(i), value)) {
      throw new IOException("oops");
    }
  }
}

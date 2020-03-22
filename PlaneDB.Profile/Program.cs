using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NMaier.PlaneDB.Profile
{
  static class Program
  {
    private static void FillDB(DirectoryInfo di, PlaneDBOptions tableoptions)
    {
      using var db = new PlaneDB(di, FileMode.CreateNew, tableoptions);
      var value = Enumerable.Repeat(Encoding.UTF8.GetBytes("hello world"), 512).SelectMany(i => i).ToArray();
      Console.WriteLine($"Writing len={value.Length:N0}");

      for (var i = 0; i < 10000; ++i) {
        if (!db.TryAdd(BitConverter.GetBytes(i), value)) {
          throw new IOException("oops");
        }
      }
    }

    static void Main()
    {
      var di = new DirectoryInfo("testdb");
      if (di.Exists) {
        di.Delete(true);
      }

      var tableoptions = new PlaneDBOptions().EnableEncryption("this is just a test");
      FillDB(di, tableoptions);
      VerifyDB(di, tableoptions);
    }

    private static void VerifyDB(DirectoryInfo di, PlaneDBOptions tableoptions)
    {
      using var db = new PlaneDB(di, FileMode.Open, tableoptions);
      for (var i = 0; i < 10000; ++i) {
        if (!db.TryGetValue(BitConverter.GetBytes(i), out _)) {
          throw new KeyNotFoundException();
        }
      }
    }
  }
}
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests
{
  [TestClass]
  public sealed class TableTests
  {
    const int COUNT = 5_000;

    [TestMethod]
    public void TestJounal()
    {
      using var ms = new KeepOpenMemoryStream();
      using (var journal = new Journal(ms, new PlaneDBOptions())) {
        for (var i = 0; i < COUNT; ++i) {
          var v = i.ToString();
          journal.Put(v, v + v + v);
          if (i % 10 == 0) {
            journal.Put("o" + v, v + v + v);
          }

          if (i % 30 == 0) {
            Assert.IsTrue(journal.Update("o" + v, v + v + v + v));
          }
          else if (i % 20 == 0) {
            journal.Remove("o" + v);
          }
        }
      }

      var table = new MemoryTable(new PlaneDBOptions());
      Journal.ReplayOnto(ms, new PlaneDBOptions(), table);

      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        Assert.IsTrue(table.ContainsKey(v, out _));
        Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
        Assert.IsTrue(table.TryGet(v, out var s));
        Assert.AreEqual(v + v + v, s);

        if (i % 30 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v + v, val);
        }
        else if (i % 20 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.IsNull(val);
        }
        else if (i % 10 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v, val);
        }
      }
    }

    [TestMethod]
    public void TestJounalBroken()
    {
      using var js = new KeepOpenMemoryStream();
      using (var journal = new Journal(js, new PlaneDBOptions())) {
        for (var i = 0; i < COUNT; ++i) {
          var v = i.ToString();
          journal.Put(v, v + v + v);
          if (i % 10 == 0) {
            journal.Put("o" + v, v + v + v);
          }

          if (i % 30 == 0) {
            Assert.IsTrue(journal.Update("o" + v, v + v + v + v));
          }
          else if (i % 20 == 0) {
            journal.Remove("o" + v);
          }
        }
      }

      js.SetLength(js.Length - 1);
      var table = new MemoryTable(new PlaneDBOptions());
      Journal.ReplayOnto(js, new PlaneDBOptions(), table);
    }

    [TestMethod]
    public void TestJounalSST()
    {
      using var cache = new BlockCache(100);
      using var scache = cache.Get(0);

      using var js = new KeepOpenMemoryStream();
      using var ms = new KeepOpenMemoryStream();
      using (var journal = new Journal(js, new PlaneDBOptions())) {
        for (var i = 0; i < COUNT; ++i) {
          var v = i.ToString();
          journal.Put(v, v + v + v);
          if (i % 10 == 0) {
            journal.Put("o" + v, v + v + v);
          }

          if (i % 30 == 0) {
            Assert.IsTrue(journal.Update("o" + v, v + v + v + v));
          }
          else if (i % 20 == 0) {
            journal.Remove("o" + v);
          }
        }
      }

      using (var builder = new SSTableBuilder(ms, new PlaneDBOptions())) {
        Journal.ReplayOnto(js, new PlaneDBOptions(), builder);
      }

      using var table = new SSTable(ms, scache, new PlaneDBOptions());
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        Assert.IsTrue(table.ContainsKey(v, out _));
        Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
        Assert.IsTrue(table.TryGet(v, out var s));
        Assert.AreEqual(v + v + v, s);

        if (i % 30 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v + v, val);
        }
        else if (i % 20 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.IsNull(val);
        }
        else if (i % 10 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v, val);
        }
      }
    }

    [TestMethod]
    public void TestJounalVarLength()
    {
      using var ms = new KeepOpenMemoryStream();
      using (var journal = new Journal(ms, new PlaneDBOptions().EnableCompression())) {
        for (var i = 0; i < COUNT; ++i) {
          var v = i.ToString();
          journal.Put(v, v + v + v);
          if (i % 10 == 0) {
            journal.Put("o" + v, v + v + v);
          }

          if (i % 30 == 0) {
            Assert.IsTrue(journal.Update("o" + v, v + v + v + v));
          }
          else if (i % 20 == 0) {
            journal.Remove("o" + v);
          }
        }
      }

      var table = new MemoryTable(new PlaneDBOptions().EnableCompression());
      Journal.ReplayOnto(ms, new PlaneDBOptions().EnableCompression(), table);

      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        Assert.IsTrue(table.ContainsKey(v, out _));
        Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
        Assert.IsTrue(table.TryGet(v, out var s));
        Assert.AreEqual(v + v + v, s);

        if (i % 30 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v + v, val);
        }
        else if (i % 20 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.IsNull(val);
        }
        else if (i % 10 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v, val);
        }
      }
    }

    [TestMethod]
    public void TestManfiestWrongOptions()
    {
      using var ms = new KeepOpenMemoryStream();
      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        for (byte l = 0; l < 128; ++l) {
          var level = new List<ulong>();
          for (var i = 0; i < COUNT; ++i) {
            var id = manifest.AllocateIdentifier();
            level.Add(id);
          }

          manifest.CommitLevel(Array.Empty<byte>(), l, level.ToArray());
        }
      }

      // ReSharper disable once AccessToDisposedClosure
      Assert.ThrowsException<BadMagicException>(() => {
        var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableEncryption("test"));
        manifest.Dispose();
      });
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void TestManifest(int familyLen)
    {
      var family = new byte[familyLen];
      using var ms = new KeepOpenMemoryStream();
      var expectedIds = new List<ulong>();
      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        for (byte l = 0; l < 128; ++l) {
          var level = new List<ulong>();
          for (var i = 0; i < COUNT; ++i) {
            var id = manifest.AllocateIdentifier();
            expectedIds.Add(id);
            level.Add(id);
          }

          manifest.CommitLevel(family, l, level.ToArray());
        }
      }

      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        var seq = manifest.Sequence(family);
        Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
      }
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void TestManifestRemoves(int familyLen)
    {
      var family = new byte[familyLen];
      using var ms = new KeepOpenMemoryStream();
      var expectedIds = new List<ulong>();
      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        for (byte l = 0; l < 128; ++l) {
          var level = new List<ulong>();
          for (var i = 0; i < COUNT; ++i) {
            var id = manifest.AllocateIdentifier();
            expectedIds.Add(id);
            level.Add(id);
          }

          manifest.CommitLevel(family, l, level.ToArray());
          level.Clear();
          manifest.CommitLevel(family, l, level.ToArray());
        }
      }

      expectedIds.Clear();

      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        var seq = manifest.Sequence(family);
        Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
      }
    }

    [TestMethod]
    [DataRow(0)]
    [DataRow(1)]
    public void TestManifestUpdates(int familyLen)
    {
      var family = new byte[familyLen];
      using var ms = new KeepOpenMemoryStream();
      var expectedIds = new List<ulong>();
      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        for (byte l = 0; l < 128; ++l) {
          var level = new List<ulong>();
          for (var i = 0; i < COUNT; ++i) {
            var id = manifest.AllocateIdentifier();
            if (i > 0) {
              expectedIds.Add(id);
            }

            level.Add(id);
          }

          manifest.CommitLevel(family, l, level.ToArray());
          level.RemoveAt(0);
          manifest.CommitLevel(family, l, level.ToArray());
        }
      }

      using (var manifest = new Manifest(new DirectoryInfo("."), ms, new PlaneDBOptions().EnableCompression())) {
        var seq = manifest.Sequence(family);
        Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
      }
    }

    [TestMethod]
    public void TestMemoryTable()
    {
      var table = new MemoryTable(new PlaneDBOptions());
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        if (i % 16 != 0) {
          table.Put(v, v + v + v);
        }

        if (i % 10 == 0) {
          table.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          Assert.IsTrue(table.Update("o" + v, v + v + v + v));
        }
        else if (i % 20 == 0) {
          table.Remove("o" + v);
        }
      }

      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        if (i % 16 != 0) {
          Assert.IsTrue(table.ContainsKey(v, out _));
          Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
          Assert.IsTrue(table.TryGet(v, out var s));
          Assert.AreEqual(v + v + v, s);
        }
        else {
          Assert.IsFalse(table.ContainsKey(v, out _));
          Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
          Assert.IsFalse(table.TryGet(v, out var s));
          Assert.AreEqual(null, s);
        }

        if (i % 30 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v + v, val);
        }
        else if (i % 20 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.IsNull(val);
        }
        else if (i % 10 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v, val);
        }
      }
    }

    [TestMethod]
    public void TestSSTableAndBuilder()
    {
      using var cache = new BlockCache(100);
      using var scache = cache.Get(0);

      using var ms = new KeepOpenMemoryStream();
      using (var builder = new SSTableBuilder(ms, new PlaneDBOptions())) {
        for (var i = 0; i < COUNT; ++i) {
          var v = i.ToString();
          if (i % 16 != 0) {
            builder.Put(v, v + v + v);
          }

          if (i % 10 == 0) {
            builder.Put("o" + v, v + v + v);
          }

          if (i % 20 == 0) {
            builder.Remove("o" + v);
          }
        }
      }

      using var table = new SSTable(ms, scache, new PlaneDBOptions());
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        if (i % 16 != 0) {
          Assert.IsTrue(table.ContainsKey(v, out _));
          Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
          Assert.IsTrue(table.TryGet(v, out var s));
          Assert.AreEqual(v + v + v, s);
        }
        else {
          Assert.IsFalse(table.ContainsKey(v, out _));
          Assert.IsFalse(table.ContainsKey($"nope{v}", out _));
          Assert.IsFalse(table.TryGet(v, out var s));
          Assert.AreEqual(null, s);
        }

        if (i % 20 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.IsNull(val);
        }
        else if (i % 10 == 0) {
          Assert.IsTrue(table.ContainsKey("o" + v, out _));
          Assert.IsTrue(table.TryGet("o" + v, out var val));
          Assert.AreEqual(v + v + v, val);
        }
      }
    }
  }
}
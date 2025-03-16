using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using NMaier.BlockStream;

namespace NMaier.PlaneDB.Tests;

[TestClass]
public sealed class TableTests
{
  private const int COUNT = 1_000;

  private static ISSTable CreateCached(Stream s, IBlockCache c, PlaneOptions o)
  {
    return new SSTableKeyCached(s, null, c, o);
  }

  private static ISSTable CreateNonCached(Stream s, IBlockCache c, PlaneOptions o)
  {
    return new SSTable(s, null, c, o);
  }

  private static void TestJournalSSTInternal(
    Func<Stream, IBlockCache, PlaneOptions, ISSTable> func)
  {
    var salt = new byte[Constants.SALT_BYTES];

    using var cache = new BlockCache(100);
    using var specificCache = cache.Get(0);

    using var js = new KeepOpenMemoryStream();
    using var ms = new KeepOpenMemoryStream();
    using (var journal = new Journal(
             js,
             new byte[Constants.SALT_BYTES],
             new PlaneOptions(),
             new FakeReadWriteLock())) {
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        journal.Put(v, v + v + v);
        if (i % 10 == 0) {
          journal.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          journal.Put("o" + v, v + v + v + v);
        }
        else if (i % 20 == 0) {
          journal.Remove("o" + v);
        }
      }
    }

    using (var builder = new SSTableBuilder(ms, salt, new PlaneOptions())) {
      Journal.ReplayOnto(js, new byte[Constants.SALT_BYTES], new PlaneOptions(), builder);
    }

    using var table = func(ms, specificCache, new PlaneOptions());
    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      Assert.IsTrue(table.ContainsKey(v));
      Assert.IsFalse(table.ContainsKey($"nope{v}"));
      Assert.IsTrue(table.TryGet(v, out var s));
      Assert.AreEqual(v + v + v, s);

      if (i % 30 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v + v, val);
      }
      else if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }
  }

  private static void TestSSTableAndBuilderInternal(
    Func<Stream, IBlockCache, PlaneOptions, ISSTable> func)
  {
    var salt = new byte[Constants.SALT_BYTES];

    using var cache = new BlockCache(100);
    using var specificCache = cache.Get(0);
    specificCache.Invalidate(0);

    using var ms = new KeepOpenMemoryStream();
    using (var builder = new SSTableBuilder(ms, salt, new PlaneOptions())) {
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

    specificCache.Invalidate(0);
    using var table = func(ms, specificCache, new PlaneOptions());
    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      if (i % 16 != 0) {
        Assert.IsTrue(table.ContainsKey(v));
        Assert.IsFalse(table.ContainsKey($"nope{v}"));
        Assert.IsTrue(table.TryGet(v, out var s));
        Assert.AreEqual(v + v + v, s);
      }
      else {
        Assert.IsFalse(table.ContainsKey(v));
        Assert.IsFalse(table.ContainsKey($"nope{v}"));
        Assert.IsFalse(table.TryGet(v, out var s));
        Assert.AreEqual(null, s);
      }

      if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }

    specificCache.Invalidate(0);
    ms.SetLength(0);
    _ = Assert.ThrowsException<EndOfStreamException>(
      () => {
        // ReSharper disable AccessToDisposedClosure
        using var t2 = func(ms, specificCache, new PlaneOptions());
        t2.EnsureLazyInit();
        // ReSharper restore AccessToDisposedClosure
      });
  }

  [TestMethod]
  public void TestJournal()
  {
    using var ms = new KeepOpenMemoryStream();
    using (var journal = new Journal(
             ms,
             new byte[Constants.SALT_BYTES],
             new PlaneOptions(),
             new FakeReadWriteLock())) {
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        journal.Put(v, v + v + v);
        if (i % 10 == 0) {
          journal.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          journal.Put("o" + v, v + v + v + v);
        }
        else if (i % 20 == 0) {
          journal.Remove("o" + v);
        }
      }
    }

    var table = new MemoryTable(new PlaneOptions(), 0);
    Journal.ReplayOnto(ms, new byte[Constants.SALT_BYTES], new PlaneOptions(), table);

    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      Assert.IsTrue(table.ContainsKey(v));
      Assert.IsFalse(table.ContainsKey($"nope{v}"));
      Assert.IsTrue(table.TryGet(v, out var s));
      Assert.AreEqual(v + v + v, s);

      if (i % 30 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v + v, val);
      }
      else if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }
  }

  [TestMethod]
  public void TestJournalBroken()
  {
    using var js = new KeepOpenMemoryStream();
    using (var journal = new Journal(
             js,
             new byte[Constants.SALT_BYTES],
             new PlaneOptions(),
             new FakeReadWriteLock())) {
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        journal.Put(v, v + v + v);
        if (i % 10 == 0) {
          journal.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          journal.Put("o" + v, v + v + v + v);
        }
        else if (i % 20 == 0) {
          journal.Remove("o" + v);
        }
      }
    }

    js.SetLength(js.Length - 1);
    var table = new MemoryTable(new PlaneOptions(), 0);
    Journal.ReplayOnto(js, new byte[Constants.SALT_BYTES], new PlaneOptions(), table);

    _ = js.Seek(10, SeekOrigin.Begin);
    js.WriteInt32(-1);
    _ = js.Seek(0, SeekOrigin.Begin);
    _ = Assert.ThrowsException<PlaneDBBrokenJournalException>(
      // ReSharper disable once AccessToDisposedClosure
      () => Journal.ReplayOnto(
        js,
        new byte[Constants.SALT_BYTES],
        new PlaneOptions(),
        table));
  }

  [TestMethod]
  public void TestJournalSST()
  {
    TestJournalSSTInternal(CreateNonCached);
    TestJournalSSTInternal(CreateCached);
  }

  [TestMethod]
  public void TestJournalTruncated()
  {
    // Prepare Journal
    using var js = new KeepOpenMemoryStream();
    using (var journal = new Journal(
             js,
             new byte[Constants.SALT_BYTES],
             new PlaneOptions(),
             new FakeReadWriteLock())) {
      for (var i = 0; i < 75; ++i) {
        var v = i.ToString();
        var bytes = new byte[(i + 1) * sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, i);
        journal.Put(bytes.AsSpan(0, sizeof(long)), bytes);
        if (i % 10 == 0) {
          journal.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          journal.Put("o" + v, v + v + v + v);
        }
        else if (i % 20 == 0) {
          journal.Remove("o" + v);
        }
      }
    }

    using var js2 = new KeepOpenMemoryStream();
    _ = js.Seek(0, SeekOrigin.Begin);
    js.CopyTo(js2);
    _ = js.Seek(0, SeekOrigin.Begin);

    while (js.Length > 1) {
      js.SetLength(js.Length - 1);
      var table = new MemoryTable(new PlaneOptions(), 0);
      try {
        Journal.ReplayOnto(js, new byte[Constants.SALT_BYTES], new PlaneOptions(), table);
      }
      catch (PlaneDBException) {
        // ignored
      }

      js.SetLength(js.Length - 1);
      js.WriteByte(0x0);
      try {
        Journal.ReplayOnto(js, new byte[Constants.SALT_BYTES], new PlaneOptions(), table);
      }
      catch (PlaneDBException) {
        // ignored
      }

      _ = js2.Seek(js.Length, SeekOrigin.Begin);
      js2.WriteByte(0x0);
      _ = js.Seek(0, SeekOrigin.Begin);
      try {
        Journal.ReplayOnto(
          js2,
          new byte[Constants.SALT_BYTES],
          new PlaneOptions(),
          table);
      }
      catch (PlaneDBException) {
        // ignored
      }
    }
  }

  [TestMethod]
  public void TestJournalUniqueMemory()
  {
    using var journal = new JournalUniqueMemory();
    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      journal.Put(v, v + v + v);
      if (i % 10 == 0) {
        journal.Put("o" + v, v + v + v);
      }

      if (i % 30 == 0) {
        journal.Put("o" + v, v + v + v + v);
      }
      else if (i % 20 == 0) {
        journal.Remove("o" + v);
      }
    }

    var table = new MemoryTable(new PlaneOptions(), 0);
    journal.CopyTo(table);

    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      Assert.IsTrue(table.ContainsKey(v));
      Assert.IsFalse(table.ContainsKey($"nope{v}"));
      Assert.IsTrue(table.TryGet(v, out var s));
      Assert.AreEqual(v + v + v, s);

      if (i % 30 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v + v, val);
      }
      else if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }
  }

  [TestMethod]
  public void TestJournalVarLength()
  {
    using var ms = new KeepOpenMemoryStream();
    using (var journal = new Journal(
             ms,
             new byte[Constants.SALT_BYTES],
             new PlaneOptions().WithCompression(),
             new FakeReadWriteLock())) {
      for (var i = 0; i < COUNT; ++i) {
        var v = i.ToString();
        journal.Put(v, v + v + v);
        if (i % 10 == 0) {
          journal.Put("o" + v, v + v + v);
        }

        if (i % 30 == 0) {
          journal.Put("o" + v, v + v + v + v);
        }
        else if (i % 20 == 0) {
          journal.Remove("o" + v);
        }
      }
    }

    var table = new MemoryTable(new PlaneOptions().WithCompression(), 0);
    Journal.ReplayOnto(
      ms,
      new byte[Constants.SALT_BYTES],
      new PlaneOptions().WithCompression(),
      table);

    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      Assert.IsTrue(table.ContainsKey(v));
      Assert.IsFalse(table.ContainsKey($"nope{v}"));
      Assert.IsTrue(table.TryGet(v, out var s));
      Assert.AreEqual(v + v + v, s);

      if (i % 30 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v + v, val);
      }
      else if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }
  }

  [TestMethod]
  [DataRow(0)]
  [DataRow(1)]
  public void TestManifest(int familyLen)
  {
    var family = new byte[familyLen];
    using var ms = new KeepOpenMemoryStream();
    var expectedIds = new List<ulong>();
    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
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

    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
      var seq = manifest.Sequence(family);
      Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
      using var ms2 = new MemoryStream();
      using var ms3 = new MemoryStream();
      ms3.WriteInt64(123);
      manifest.CompactManifest(ms2);
      manifest.CompactManifest(ms3);
      Assert.IsTrue(
        ms2.ToArray()
          .AsSpan((sizeof(int) * 2) + Constants.SALT_BYTES)
          .SequenceEqual(ms3.ToArray().AsSpan((sizeof(int) * 2) + Constants.SALT_BYTES)));
    }

    using (var manifest = new ManifestReadOnly(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
      var seq = manifest.Sequence(family);
      Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
    }
  }

  [TestMethod]
  [DataRow(typeof(Manifest))]
  [DataRow(typeof(ManifestReadOnly))]
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "jb")]
  public void TestManifestBroken(Type manifestType)
  {
    using var ms = new KeepOpenMemoryStream();
    var opts = new PlaneOptions();
    var family = Encoding.UTF32.GetBytes("family");

    using (var manifest = new Manifest(new DirectoryInfo("."), ms, opts)) {
      for (byte l = 0; l < 128; ++l) {
        var level = new List<ulong>();
        for (var i = 0; i < COUNT; ++i) {
          var id = manifest.AllocateIdentifier();
          level.Add(id);
        }

        manifest.CommitLevel([], l, level.ToArray());
        manifest.CommitLevel(family, l, level.ToArray());
      }
    }

    _ = Assert.ThrowsException<PlaneDBBadMagicException>(
      () => {
        var manifest = OpenManifest(
          new DirectoryInfo("."),
          ms,
          opts.WithEncryption("test"));
        manifest.Dispose();
      },
      $"Throws on wrong transformer ({manifestType})");

    ms.SetLength(ms.Length - 1);
    _ = Assert.ThrowsException<EndOfStreamException>(
      () => {
        var manifest = OpenManifest(new DirectoryInfo("."), ms, opts);
        manifest.Dispose();
      },
      $"Throws on truncation ({manifestType})");

    if (manifestType != typeof(ManifestReadOnly)) {
      _ = Assert.ThrowsException<NotSupportedException>(
        () => {
          var manifest = OpenManifest(
            new DirectoryInfo("."),
            ms,
            opts.ActivateRepairMode());
          manifest.Dispose();

          throw new NotSupportedException();
        },
        $"Allows truncation in repair mode ({manifestType})");
    }

    _ = ms.Seek(14, SeekOrigin.Begin);
    var salt = ms.ReadFullBlock(Constants.SALT_BYTES);
    var transformed = opts.GetTransformerFor(salt)
      .TransformBlock(
      [
        0,
        1,
        2,
        3
      ]);
    ms.WriteInt32(transformed.Length);
    ms.Write(transformed);

    _ = Assert.ThrowsException<PlaneDBBadMagicException>(
      () => {
        var manifest = OpenManifest(new DirectoryInfo("."), ms, opts);
        manifest.Dispose();
      },
      $"Throws on wrong magic2 ({manifestType})");

    _ = ms.Seek(12, SeekOrigin.Begin);
    ms.WriteInt32(-1);
    _ = ms.Seek(0, SeekOrigin.Begin);
    _ = Assert.ThrowsException<PlaneDBBadMagicException>(
      () => {
        var manifest = OpenManifest(new DirectoryInfo("."), ms, opts);
        manifest.Dispose();
      },
      $"Throws on wrong magic2 length (negative) ({manifestType})");

    _ = ms.Seek(12, SeekOrigin.Begin);
    ms.WriteInt32(ushort.MaxValue + 1);
    _ = ms.Seek(0, SeekOrigin.Begin);
    _ = Assert.ThrowsException<PlaneDBBadMagicException>(
      () => {
        var manifest = OpenManifest(new DirectoryInfo("."), ms, opts.WithCompression());
        manifest.Dispose();
      },
      $"Throws on wrong magic2 length (large) ({manifestType})");

    _ = ms.Seek(0, SeekOrigin.Begin);
    ms.WriteInt32(Constants.MAGIC + 1);
    _ = ms.Seek(0, SeekOrigin.Begin);
    _ = Assert.ThrowsException<PlaneDBBadMagicException>(
      () => {
        var manifest = OpenManifest(new DirectoryInfo("."), ms, opts.WithCompression());
        manifest.Dispose();
      },
      $"Throws on wrong magic2 ({manifestType})");

    return;

    IManifest OpenManifest(DirectoryInfo d, Stream s, PlaneOptions o)
    {
      try {
        return (IManifest)Activator.CreateInstance(
          manifestType,
          BindingFlags.NonPublic | BindingFlags.Instance,
          null,
          [
            d,
            s,
            o
          ],
          null)!;
      }
      catch (TargetInvocationException ex) {
        throw ex.InnerException ?? ex;
      }
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
    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
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

    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
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
    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
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

    using (var manifest = new Manifest(
             new DirectoryInfo("."),
             ms,
             new PlaneOptions().WithCompression())) {
      var seq = manifest.Sequence(family);
      Assert.IsTrue(expectedIds.SequenceEqual(seq.OrderBy(i => i)));
    }
  }

  [TestMethod]
  public void TestMemoryTable()
  {
    var table = new MemoryTable(new PlaneOptions(), 0);
    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      if (i % 16 != 0) {
        table.Put(v, v + v + v);
      }

      if (i % 10 == 0) {
        table.Put("o" + v, v + v + v);
      }

      if (i % 30 == 0) {
        table.Put("o" + v, v + v + v + v);
      }
      else if (i % 20 == 0) {
        table.Remove("o" + v);
      }
    }

    for (var i = 0; i < COUNT; ++i) {
      var v = i.ToString();
      if (i % 16 != 0) {
        Assert.IsTrue(table.ContainsKey(v));
        Assert.IsFalse(table.ContainsKey($"nope{v}"));
        Assert.IsTrue(table.TryGet(v, out var s));
        Assert.AreEqual(v + v + v, s);
      }
      else {
        Assert.IsFalse(table.ContainsKey(v));
        Assert.IsFalse(table.ContainsKey($"nope{v}"));
        Assert.IsFalse(table.TryGet(v, out var s));
        Assert.AreEqual(null, s);
      }

      if (i % 30 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v + v, val);
      }
      else if (i % 20 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.IsNull(val);
      }
      else if (i % 10 == 0) {
        Assert.IsTrue(table.ContainsKey("o" + v));
        Assert.IsTrue(table.TryGet("o" + v, out var val));
        Assert.AreEqual(v + v + v, val);
      }
    }
  }

  [TestMethod]
  public void TestSSTableAndBuilder()
  {
    TestSSTableAndBuilderInternal(CreateNonCached);
    TestSSTableAndBuilderInternal(CreateCached);
  }
}

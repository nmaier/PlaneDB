using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Microsoft.VisualStudio.TestTools.UnitTesting;

// ReSharper disable CollectionNeverUpdated.Local
// ReSharper disable CollectionNeverQueried.Local
namespace NMaier.PlaneDB.Tests;

[TestClass]
public sealed class PlaneSetTests
{
  private const int COUNT = 5_000;

  private static readonly PlaneOptions planeOptions =
    new PlaneOptions(PlaneOpenMode.ExistingReadWrite).WithCompression();

  private static readonly string testDB = $"{Path.GetTempPath()}/PlaneTestDB";

  [TestInitialize]
  public void Initialize()
  {
    var di = new DirectoryInfo(testDB);
    if (di.Exists) {
      di.Delete(true);
    }
  }

  [TestMethod]
  [DataRow(CompactionMode.Normal)]
  [DataRow(CompactionMode.Fully)]
  public void TestBasicIterators(CompactionMode mode)
  {
    var di = new DirectoryInfo(testDB);
    using (var db = new PlaneSet(
             di,
             planeOptions.DisableJournal().WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i)));
      }
    }

    using (var db = new PlaneSet(di, planeOptions.DisableJournal())) {
      db.Compact(mode);
    }

    using (var db = new TypedPlaneSet<byte[]>(
             new PlanePassthroughSerializer(),
             di,
             planeOptions.DisableJournal())) {
      db.Compact(mode);
    }

    using (var db = new StringPlaneSet(di, planeOptions.DisableJournal())) {
      db.Compact(mode);
    }

    int read;
    using (var db = new PlaneSet(di, planeOptions.DisableJournal())) {
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.Remove(BitConverter.GetBytes(1000)));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
      read = db.Select(e => e).Count();

      Assert.AreEqual(db.Count, read);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      db.MassInsert(() => { });
      Assert.AreEqual(1, db.MassInsert(() => 1));

      Assert.IsFalse(string.IsNullOrEmpty(db.Location.FullName));
    }

    using (var db = new PlaneSet(di, planeOptions.DisableJournal())) {
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
      Assert.AreEqual(9999, db.Count);
      read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneSet(di, planeOptions.DisableJournal())) {
      Assert.IsFalse(db.IsReadOnly);
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.AreEqual(9999, db.Select(_ => 1).Count());
      Assert.AreEqual(9999, db.ToArray().Length);
      Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
      Assert.AreEqual(9999, db.Count);
      read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      // ReSharper disable AccessToDisposedClosure
      var array = new byte[read + 100][];
      db.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(() => db.CopyTo(array, 1000));
      ICollection<byte[]> coll = db;
      coll.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(
        () => coll.CopyTo(array, 1000));
      IProducerConsumerCollection<byte[]> prodColl = db;
      prodColl.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(
        () => prodColl.CopyTo(array, 1000));
      // ReSharper restore AccessToDisposedClosure

      Assert.AreEqual(db.Count, read);
    }

    using (var db = new TypedPlaneSet<byte[]>(
             new PlanePassthroughSerializer(),
             di,
             planeOptions.DisableJournal())) {
      Assert.AreEqual(db.Count, read);
      Assert.IsFalse(db.IsReadOnly);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      db.MassInsert(() => { });
      Assert.AreEqual(1, db.MassInsert(() => 1));

      // ReSharper disable AccessToDisposedClosure
      var array = new byte[read + 100][];
      db.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(() => db.CopyTo(array, 1000));
      ICollection<byte[]> coll = db;
      coll.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(
        () => coll.CopyTo(array, 1000));
      IProducerConsumerCollection<byte[]> prodColl = db;
      prodColl.CopyTo(array, 100);
      Assert.IsTrue(array.Skip(100).SequenceEqual(db, planeOptions.Comparer));
      _ = Assert.ThrowsException<IndexOutOfRangeException>(
        () => prodColl.CopyTo(array, 1000));
      // ReSharper restore AccessToDisposedClosure

      var first = db.First();
      Assert.IsTrue(db.TryTake(out var other));
      Assert.IsTrue(first.AsSpan().SequenceEqual(other));
      Assert.IsFalse(first.AsSpan().SequenceEqual(db.First()));
      db.Flush();
      Assert.AreEqual(db.Count, read - 1);
      Assert.AreEqual(db.Select(_ => 1).Count(), read - 1);
      Assert.AreEqual(db.ToArray().Length, read - 1);
      db.Clear();
      Assert.AreEqual(0, db.Count);
      Assert.IsFalse(db.TryTake(out _));
    }

    using (var db = new PlaneSet(
             di,
             planeOptions.DisableJournal().WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual(0, db.Count);
      Assert.IsFalse(db.TryTake(out _));
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentTableCount);
      Assert.AreEqual(0, db.CurrentRealSize);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
      Assert.IsTrue(db.IsReadOnly);
    }

    using (var db = new TypedPlaneSet<byte[]>(
             new PlanePassthroughSerializer(),
             di,
             planeOptions.DisableJournal().WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsFalse(string.IsNullOrEmpty(db.Location.FullName));
      Assert.IsFalse(db.TryTake(out _));
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentTableCount);
      Assert.AreEqual(0, db.CurrentRealSize);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
      Assert.IsTrue(db.IsReadOnly);
    }

    using (var db = new TypedPlaneSet<byte[]>(
             new PlanePassthroughSerializer(),
             di,
             planeOptions.DisableJournal())) {
      db.Compact(mode);
    }

    using (var db = new TypedPlaneSet<byte[]>(
             new PlanePassthroughSerializer(),
             di,
             planeOptions.DisableJournal().WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsFalse(string.IsNullOrEmpty(db.Location.FullName));
      Assert.IsFalse(db.TryTake(out _));
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentTableCount);
      Assert.AreEqual(0, db.CurrentRealSize);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
      Assert.IsTrue(db.IsReadOnly);
    }
  }

  [TestMethod]
  public void TestExceptWith()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "c",
      "3",
      "k",
      "l",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      hs3.ExceptWith(hs2);
      db.ExceptWith(hs2);
      Assert.IsTrue(
        db.OrderBy(i => i).SequenceEqual(hs3.OrderBy(i => i), StringComparer.Ordinal));
    }

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      hs3.ExceptWith(hs2);
      db.ExceptWith(hs2);
      Assert.IsTrue(
        db.OrderBy(i => i).SequenceEqual(hs3.OrderBy(i => i), StringComparer.Ordinal));
    }

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      hs3.ExceptWith(hs2);
      db.ExceptWith(hs2);
      Assert.IsTrue(
        db.OrderBy(i => i).SequenceEqual(hs3.OrderBy(i => i), StringComparer.Ordinal));
    }
  }

  [TestMethod]
  public void TestIntersectWith()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "a",
      "c",
      "d",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      hs3.IntersectWith(hs2);
      db.UnionWith(hs1);
      db.IntersectWith(hs2);
      var a = string.Join(", ", hs2.OrderBy(i => i, StringComparer.Ordinal));
      var b = string.Join(", ", db.OrderBy(i => i, StringComparer.Ordinal));
      Assert.IsTrue(hs3.SetEquals(hs2));
      Assert.IsTrue(db.SetEquals(hs2), $"{a} / {b}");
    }

    _ = hs2.Add("g");

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      hs3.IntersectWith(hs2);
      db.UnionWith(hs1);
      db.IntersectWith(hs2);
      Assert.IsFalse(hs3.SetEquals(hs2));
      Assert.IsFalse(db.SetEquals(hs2));
      Assert.IsTrue(hs3.SetEquals(db));
      Assert.IsTrue(db.SetEquals(hs3));
    }
  }

  [TestMethod]
  public void TestIsProperSubsetOf()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "c",
      "d",
      "f"
    };
    var hs2 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs1.IsProperSubsetOf(hs2));
      Assert.IsTrue(db.IsProperSubsetOf(hs2));
    }

    hs1.UnionWith(hs2);

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsFalse(hs1.IsProperSubsetOf(hs2));
      Assert.IsFalse(db.IsProperSubsetOf(hs2));
    }

    _ = hs1.Add("g");
    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsFalse(hs1.IsProperSubsetOf(hs2));
      Assert.IsFalse(db.IsProperSubsetOf(hs2));
    }
  }

  [TestMethod]
  public void TestIsProperSupersetOf()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "a",
      "c",
      "d",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs1.IsProperSupersetOf(hs2));
      Assert.IsTrue(db.IsProperSupersetOf(hs2));
    }

    hs2.UnionWith(hs1);

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      Assert.IsFalse(hs1.IsProperSupersetOf(hs2));
      Assert.IsFalse(db.IsProperSupersetOf(hs2));
    }

    _ = hs2.Add("g");

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      Assert.IsFalse(hs1.IsProperSupersetOf(hs2));
      Assert.IsFalse(db.IsProperSupersetOf(hs2));
    }
  }

  [TestMethod]
  public void TestIsSubsetOf()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "c",
      "d",
      "f"
    };
    var hs2 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs1.IsSubsetOf(hs2));
      Assert.IsTrue(db.IsSubsetOf(hs2));
    }

    hs1.UnionWith(hs2);

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      Assert.IsTrue(hs1.IsSubsetOf(hs2));
      Assert.IsTrue(db.IsSubsetOf(hs2));
    }

    _ = hs1.Add("g");
    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsFalse(hs1.IsSubsetOf(hs2));
      Assert.IsFalse(db.IsSubsetOf(hs2));
    }
  }

  [TestMethod]
  public void TestIsSupersetOf()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "a",
      "c",
      "d",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs1.IsSupersetOf(hs2));
      Assert.IsTrue(db.IsSupersetOf(hs2));
    }

    hs2.UnionWith(hs1);

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      Assert.IsTrue(hs1.IsSupersetOf(hs2));
      Assert.IsTrue(db.IsSupersetOf(hs2));
    }
  }

  [TestMethod]
  public void TestLargeish()
  {
    var di = new DirectoryInfo(testDB);
    using (var db = new PlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i)));
      }
    }

    using (var db = new PlaneSet(di, planeOptions)) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.Contains(BitConverter.GetBytes(i)));
      }
    }

    using (var db = new PlaneSet(di, planeOptions)) {
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.Remove(BitConverter.GetBytes(1000)));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
      var read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      Assert.AreEqual(db.Count, read);
      db.RegisterMergeParticipant(new NullMergeParticipant<byte[]>());
      db.RegisterMergeParticipant(new NullMergeParticipant<byte[]>());
      db.Compact();
      Assert.AreEqual(db.Count, read);
      db.RegisterMergeParticipant(new KillAllMergeParticipant<byte[]>());
      db.RegisterMergeParticipant(new NullMergeParticipant<byte[]>());
      db.RegisterMergeParticipant(new NullMergeParticipant<byte[]>());
      db.Compact();
      Assert.AreEqual(0, db.Count);
      db.Clear();
      Assert.AreEqual(0, db.Count);
    }

    using (var db = new PlaneSet(di, planeOptions)) {
      Assert.AreEqual(0, db.Count);
    }
  }

  [TestMethod]
  public void TestOverlaps()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string>();
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsFalse(hs3.Overlaps(hs2));
      Assert.IsFalse(db.Overlaps(hs2));
    }

    _ = hs2.Add("g");

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsFalse(hs3.Overlaps(hs2));
      Assert.IsFalse(db.Overlaps(hs2));
    }

    _ = hs2.Add("a");

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs3.Overlaps(hs2));
      Assert.IsTrue(db.Overlaps(hs2));
    }

    hs2.Clear();
    hs2.UnionWith(hs1);

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(hs3.Overlaps(hs2));
      Assert.IsTrue(db.Overlaps(hs2));
    }
  }

  [TestMethod]
  public void TestSet()
  {
    using (var db = new PlaneSet(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.TryAdd(k));
      }
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.Contains(k));
      }

      Assert.IsTrue(db.Remove("0"u8.ToArray()));
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 1; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.Contains(k));
      }
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        if (i == 0) {
          Assert.IsFalse(db.Contains(k));
        }
        else {
          Assert.IsTrue(db.Contains(k));
        }
      }
    }
  }

  [TestMethod]
  [DataRow(2)]
  [DataRow(4)]
  [DataRow(8)]
  [DataRow(32)]
  public void TestSetConcurrent(int concurrency)
  {
    var j = 0;
    using (var db = new PlaneSet(
             new DirectoryInfo(testDB),
             planeOptions.UsingTablespace(concurrency.ToString())
               .WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      void Adder()
      {
        int i;
        while ((i = Interlocked.Increment(ref j)) < COUNT) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.TryAdd(k));
        }
      }

      var threads = Enumerable.Range(0, concurrency)
        .Select(_ => new Thread(Adder))
        .ToArray();
      foreach (var thread in threads) {
        thread.Start();
      }

      foreach (var thread in threads) {
        thread.Join();
      }
    }

    j = 0;
    using (var db = new PlaneSet(
             new DirectoryInfo(testDB),
             planeOptions.UsingTablespace(concurrency.ToString()))) {
      void Reader()
      {
        int i;
        while ((i = Interlocked.Increment(ref j)) < COUNT) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.Contains(k));
        }
      }

      var threads = Enumerable.Range(0, concurrency)
        .Select(_ => new Thread(Reader))
        .ToArray();
      foreach (var thread in threads) {
        thread.Start();
      }

      foreach (var thread in threads) {
        thread.Join();
      }
    }
  }

  [TestMethod]
  public void TestSetThreadUnsafe()
  {
    var opts = planeOptions.DisableThreadSafety();
    using (var db = new PlaneSet(
             new DirectoryInfo(testDB),
             opts.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.TryAdd(k));
      }
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), opts)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.Contains(k));
      }

      Assert.IsTrue(db.Remove("0"u8.ToArray()));
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), opts)) {
      for (var i = 1; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        Assert.IsTrue(db.Contains(k));
      }
    }

    using (var db = new PlaneSet(new DirectoryInfo(testDB), opts)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        if (i == 0) {
          Assert.IsFalse(db.Contains(k));
        }
        else {
          Assert.IsTrue(db.Contains(k));
        }
      }
    }
  }

  [TestMethod]
  public void TestSymmetricExceptWith()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "c",
      "3",
      "k",
      "l",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      hs3.SymmetricExceptWith(hs2);
      db.SymmetricExceptWith(hs2);
      var s1 = string.Join(", ", db.OrderBy(i => i));
      var s2 = string.Join(", ", hs3.OrderBy(i => i));
      Assert.IsTrue(db.SetEquals(hs3), $"{s1} / {s2}");
      Assert.IsTrue(hs3.SetEquals(db), $"{s1} / {s2}");
    }

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.Clear();
      db.UnionWith(hs1);
      var s1 = string.Join(", ", db.OrderBy(i => i));
      var s2 = string.Join(", ", hs3.OrderBy(i => i));
      Assert.IsTrue(db.SetEquals(hs3), $"{s1} / {s2}");
      Assert.IsTrue(hs3.SetEquals(db), $"{s1} / {s2}");

      hs3.SymmetricExceptWith(hs2);
      db.SymmetricExceptWith(hs2);
      s1 = string.Join(", ", db.OrderBy(i => i));
      s2 = string.Join(", ", hs3.OrderBy(i => i));
      Assert.IsTrue(db.SetEquals(hs3), $"{s1} / {s2}");
      Assert.IsTrue(hs3.SetEquals(db), $"{s1} / {s2}");
    }
  }

  [TestMethod]
  public void TestTyped()
  {
    using (var db = new StringPlaneSet(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        Assert.IsTrue(db.TryAdd(k));
      }
    }

    using (var db = new StringPlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        Assert.IsTrue(db.Contains(k));
      }

      Assert.IsTrue(db.Remove("0"));
    }

    using (var db = new StringPlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 1; i < COUNT; ++i) {
        var k = i.ToString();
        Assert.IsTrue(db.Contains(k));
      }
    }

    using (var db = new StringPlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        if (i == 0) {
          Assert.IsFalse(db.Contains(k));
        }
        else {
          Assert.IsTrue(db.Contains(k));
        }
      }
    }

    using (var db = new StringPlaneSet(new DirectoryInfo(testDB), planeOptions)) {
      var c = db.Count;
      Assert.AreNotEqual(0, c);
      db.RegisterMergeParticipant(new NullMergeParticipant<string>());
      db.RegisterMergeParticipant(new NullMergeParticipant<string>());
      db.Compact();
      Assert.AreEqual(c, db.Count);
      db.RegisterMergeParticipant(new KillAllMergeParticipant<string>());
      db.RegisterMergeParticipant(new NullMergeParticipant<string>());
      db.RegisterMergeParticipant(new NullMergeParticipant<string>());
      db.Compact();
      Assert.AreEqual(0, db.Count);
      db.Clear();
      Assert.AreEqual(0, db.Count);
    }
  }

  [TestMethod]
  public void TestUnionWith()
  {
    var di = new DirectoryInfo(testDB);
    var hs1 = new HashSet<string> {
      "a",
      "b",
      "c",
      "d",
      "e",
      "f"
    };
    var hs2 = new HashSet<string> {
      "c",
      "3",
      "k",
      "l",
      "f"
    };
    using (var db = new StringPlaneSet(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      db.UnionWith(hs1);
      Assert.IsTrue(
        db.OrderBy(i => i).SequenceEqual(hs3.OrderBy(i => i), StringComparer.Ordinal));
      Assert.IsTrue(hs1.SetEquals(hs3));
      Assert.IsTrue(hs1.SetEquals(db));
      Assert.IsTrue(db.SetEquals(hs1));
      Assert.IsTrue(db.SetEquals(hs3));
      Assert.IsFalse(db.SetEquals(hs2));
    }

    using (var db = new StringPlaneSet(di, planeOptions)) {
      var hs3 = new HashSet<string>();
      hs3.UnionWith(hs1);
      hs3.UnionWith(hs2);
      db.UnionWith(hs1);
      db.UnionWith(hs2);
      Assert.IsTrue(
        db.OrderBy(i => i).SequenceEqual(hs3.OrderBy(i => i), StringComparer.Ordinal));
      Assert.IsTrue(hs3.SetEquals(db));
      Assert.IsTrue(db.SetEquals(hs3));
      Assert.IsFalse(db.SetEquals(hs2));
    }
  }

  private sealed class KillAllMergeParticipant<TKey> : IPlaneSetMergeParticipant<TKey>
  {
    public bool Equals(IPlaneSetMergeParticipant<TKey>? other)
    {
      return ReferenceEquals(this, other);
    }

    public bool IsDataStale(in TKey key)
    {
      return true;
    }
  }

  private sealed class NullMergeParticipant<TKey> : IPlaneSetMergeParticipant<TKey>
  {
    public bool Equals(IPlaneSetMergeParticipant<TKey>? other)
    {
      return ReferenceEquals(this, other);
    }

    public bool IsDataStale(in TKey key)
    {
      return false;
    }
  }
}

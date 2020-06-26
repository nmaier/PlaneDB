using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests
{
  [TestClass]
  public sealed class PlaneSetTests
  {
    private const int COUNT = 10_000;

    private static readonly PlaneDBOptions planeDBOptions = new PlaneDBOptions().EnableCompression();

    [TestInitialize]
    public void Initialize()
    {
      var di = new DirectoryInfo("testdb");
      if (di.Exists) {
        di.Delete(true);
      }
    }


    [TestMethod]
    public void TestBasicIterators()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = new PlaneSet(di, FileMode.CreateNew, planeDBOptions.DisableJournal())) {
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i)));
        }
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        db.Compact();
      }

      int read;
      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 10000);
        Assert.IsTrue(db.Remove(BitConverter.GetBytes(1000)));
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
        read = db.Select(e => e).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
        Assert.AreEqual(db.Count, 9999);
        read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
        Assert.AreEqual(db.Count, 9999);
        read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, read);
        db.Clear();
        Assert.AreEqual(db.Count, 0);
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 0);
      }
    }

    [TestMethod]
    public void TestLargeish()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = new PlaneSet(di, FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i)));
        }
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.Contains(BitConverter.GetBytes(i)));
        }
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions)) {
        Assert.AreEqual(db.Count, 10000);
        Assert.IsTrue(db.Remove(BitConverter.GetBytes(1000)));
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.Remove(BitConverter.GetBytes(1000)));
        var read = db.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

        Assert.AreEqual(db.Count, read);
        db.Clear();
        Assert.AreEqual(db.Count, 0);
      }

      using (var db = new PlaneSet(di, FileMode.Open, planeDBOptions)) {
        Assert.AreEqual(db.Count, 0);
      }
    }

    [TestMethod]
    public void TestRemoveOrphans()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = new PlaneSet(di, FileMode.CreateNew, planeDBOptions.DisableJournal())) {
        for (var i = 0; i < 10; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i)));
        }
      }

      var junk = Path.Combine(di.FullName, "default-119191919191.planedb");
      File.WriteAllText(junk, "test");
      using (new PlaneSet(di, FileMode.OpenOrCreate, planeDBOptions)) {
        Assert.IsFalse(File.Exists(junk));
      }
    }

    [TestMethod]
    public void TestSet()
    {
      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.TryAdd(k));
        }
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.Contains(k));
        }

        Assert.IsTrue(db.Remove(Encoding.UTF8.GetBytes("0")));
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.Contains(k));
        }
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
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
      int j = 0;
      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.CreateNew,
                                   planeDBOptions.UsingTableSpace(concurrency.ToString()))) {
        void Adder()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            Assert.IsTrue(db.TryAdd(k));
          }
        }

        var threads = Enumerable.Range(0, concurrency).Select(_ => new Thread(Adder)).ToArray();
        foreach (var thread in threads) {
          thread.Start();
        }

        foreach (var thread in threads) {
          thread.Join();
        }
      }

      j = 0;
      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open,
                                   planeDBOptions.UsingTableSpace(concurrency.ToString()))) {
        void Reader()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            Assert.IsTrue(db.Contains(k));
          }
        }

        var threads = Enumerable.Range(0, concurrency).Select(_ => new Thread(Reader)).ToArray();
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
      var opts = planeDBOptions.DisableThreadSafety();
      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.CreateNew, opts)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.TryAdd(k));
        }
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.Contains(k));
        }

        Assert.IsTrue(db.Remove(Encoding.UTF8.GetBytes("0")));
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          Assert.IsTrue(db.Contains(k));
        }
      }

      using (var db = new PlaneSet(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
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
    public void TestTyped()
    {
      using (var db = new StringPlaneSet(new DirectoryInfo("testdb"), FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = i.ToString();
          Assert.IsTrue(db.TryAdd(k));
        }
      }

      using (var db = new StringPlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = i.ToString();
          Assert.IsTrue(db.Contains(k));
        }

        Assert.IsTrue(db.Remove("0"));
      }

      using (var db = new StringPlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = i.ToString();
          Assert.IsTrue(db.Contains(k));
        }
      }

      using (var db = new StringPlaneSet(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
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
    }
  }
}
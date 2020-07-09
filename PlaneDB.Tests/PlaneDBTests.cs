using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;

#if TEST_ROCKS
using RocksDbSharp;
#endif

namespace NMaier.PlaneDB.Tests
{
  [TestClass]
  public sealed class PlaneDBTests
  {
    private const int COUNT = 10_000;

    private static readonly PlaneDBOptions planeDBOptions = new PlaneDBOptions().EnableCompression();
#if TEST_ROCKS
    private DbOptions rocksOptions;
#endif

    [TestInitialize]
    public void Initialize()
    {
      var di = new DirectoryInfo("testdb");
      if (di.Exists) {
        di.Delete(true);
      }

#if TEST_ROCKS
      rocksOptions = new DbOptions().SetCreateIfMissing();
      rocksOptions.OptimizeForPointLookup(8);
#endif
    }


    [TestMethod]
    public void TestAlreadyLocked()
    {
      Assert.ThrowsException<AlreadyLockedException>(() => {
        using var _ = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.OpenOrCreate, planeDBOptions);
        using var __ = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.OpenOrCreate, planeDBOptions);
      });
    }

    [TestMethod]
    public void TestBasicIterators()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = new PlaneDB(di, FileMode.CreateNew, planeDBOptions.DisableJournal())) {
        var value = new byte[100];
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
        }
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        db.Compact();
        // XXX Verify table count and sequence are correct
      }

      int read;
      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 10000);
        Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
        read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
        Assert.AreEqual(db.Count, 9999);
        read = db.KeysIterator.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
        Assert.AreEqual(db.Count, 9999);
        read = db.Keys.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

        Assert.AreEqual(db.Count, read);
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, read);
        db.Clear();
        Assert.AreEqual(db.Count, 0);
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions.DisableJournal())) {
        Assert.AreEqual(db.Count, 0);
      }
    }

    [TestMethod]
    public void TestLargeish()
    {
      var di = new DirectoryInfo("testdb");
      var value = new byte[4096];
      value[0] = 1;
      value[4095] = 0xff;
      using (var db = new PlaneDB(di, FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
        }
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < 10000; ++i) {
          Assert.IsTrue(db.TryGetValue(BitConverter.GetBytes(i), out var val));
          Assert.IsTrue(value.AsSpan().SequenceEqual(val));
        }
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions)) {
        Assert.AreEqual(db.Count, 10000);
        Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
        Assert.AreEqual(db.Count, 9999);
        Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
        var read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

        Assert.AreEqual(db.Count, read);
        db.Clear();
        Assert.AreEqual(db.Count, 0);
      }

      using (var db = new PlaneDB(di, FileMode.Open, planeDBOptions)) {
        Assert.AreEqual(db.Count, 0);
      }
    }

#if TEST_ROCKS
    [TestMethod]
    public void TestLargeishRocks()
    {
      var di = new DirectoryInfo("testdb");
      var value = new byte[4096];
      value[0] = 1;
      value[4095] = 0xff;
      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 0; i < 10000; ++i) {
          db.Put(BitConverter.GetBytes(i), value);
        }
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 0; i < 10000; ++i) {
          var val = db.Get(BitConverter.GetBytes(i));
          Assert.IsTrue(val != null);
          Assert.IsTrue(value.AsSpan().SequenceEqual(val));
        }
      }
    }
#endif

    [TestMethod]
    public void TestRemoveOrphans()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = new PlaneDB(di, FileMode.CreateNew, planeDBOptions.DisableJournal())) {
        var value = new byte[100];
        for (var i = 0; i < 10; ++i) {
          Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
        }
      }

      var junk = Path.Combine(di.FullName, "default-119191919191.planedb");
      File.WriteAllText(junk, "test");
      using (new PlaneDB(di, FileMode.OpenOrCreate, planeDBOptions)) {
        Assert.IsFalse(File.Exists(junk));
      }
    }

    [TestMethod]
    public void TestSet()
    {
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          db[k] = v;
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }

        Assert.IsTrue(db.TryRemove(Encoding.UTF8.GetBytes("0"), out var removed));
        Assert.AreEqual("00000", Encoding.UTF8.GetString(removed));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.IsTrue(db.ContainsKey(k));
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          if (i == 0) {
            Assert.IsFalse(db.ContainsKey(k));
            Assert.IsFalse(db.TryGetValue(k, out _));
          }
          else {
            Assert.IsTrue(db.ContainsKey(k));
            Assert.IsTrue(db.TryGetValue(k, out var v));
            Assert.AreEqual(Encoding.UTF8.GetString(v), i.ToString() + i + i + i + i);
          }
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add(Encoding.UTF8.GetBytes("test1"), Encoding.UTF8.GetBytes("test1"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test2"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.AddOrUpdate(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test3"),
                       (_, __) => Encoding.UTF8.GetBytes("test3"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add(Encoding.UTF8.GetBytes("test3"), Encoding.UTF8.GetBytes("test4"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        foreach (var i in new[] {
          new[] { "test1", "test1" }, new[] { "test2", "test3" }, new[] { "test3", "test4" }
        }) {
          Assert.IsTrue(db.ContainsKey(Encoding.UTF8.GetBytes(i[0])));
          Assert.IsTrue(db.TryGetValue(Encoding.UTF8.GetBytes(i[0]), out var v));
          Assert.AreEqual(i[1], Encoding.UTF8.GetString(v));
        }
      }
    }

    [TestMethod]
    public void TestGetOrAddRange()
    {
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, planeDBOptions)) {
        foreach (var keyValuePair in db.GetOrAddRange(Enumerable.Range(0, COUNT).Where(i => (i % 2) == 0).Select(
                                             i => new KeyValuePair<byte[], byte[]>(Encoding.UTF8.GetBytes(i.ToString()),
                                               Encoding.UTF8.GetBytes(i.ToString() + i))))) {
          var key = Encoding.UTF8.GetString(keyValuePair.Key);
          var val = Encoding.UTF8.GetString(keyValuePair.Value);
          Assert.AreEqual(key + key, val);
        }
      }
 
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.OpenOrCreate, planeDBOptions)) {
        foreach (var keyValuePair in db.GetOrAddRange(Enumerable.Range(0, COUNT).Select(i => Encoding.UTF8.GetBytes(i.ToString())), bytes => Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(bytes) + "i"))) {
          var key = Encoding.UTF8.GetString(keyValuePair.Key);
          var i = int.Parse(key);
          var val = Encoding.UTF8.GetString(keyValuePair.Value);
          if ((i % 2) == 0) {
            Assert.AreEqual(key + key, val);
          }
          else {
            Assert.AreEqual(key + "i", val);
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
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew,
                                  planeDBOptions.UsingTableSpace(concurrency.ToString()))) {
        void Adder()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
            db[k] = v;
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
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open,
                                  planeDBOptions.UsingTableSpace(concurrency.ToString()))) {
        void Reader()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            var v = Encoding.UTF8.GetString(db[k]);
            Assert.AreEqual(v, i.ToString() + i + i + i + i);
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
    [DataRow(32)]
    public void TestSetConcurrentSharedLock(int concurrency)
    {
      var options = planeDBOptions.UsingDefaultLock();
      int j = 0;
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew,
                                  options.UsingTableSpace(concurrency.ToString()))) {
        void Adder()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
            db[k] = v;
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
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open,
                                  options.UsingTableSpace(concurrency.ToString()))) {
        void Reader()
        {
          int i;
          while ((i = Interlocked.Increment(ref j)) < COUNT) {
            var k = Encoding.UTF8.GetBytes(i.ToString());
            var v = Encoding.UTF8.GetString(db[k]);
            Assert.AreEqual(v, i.ToString() + i + i + i + i);
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
    public void TestSetFullySync()
    {
      var options = planeDBOptions.MakeFullySync();
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, options)) {
        for (var i = 0; i < 10; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          db[k] = v;
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, options)) {
        for (var i = 0; i < 10; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }
      }
    }
    
    [TestMethod]
    public void TestSetMostlySync()
    {
      var options = planeDBOptions.MakeMostlySync();
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, options)) {
        for (var i = 0; i < 100; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          db[k] = v;
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, options)) {
        for (var i = 0; i < 100; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }
      }
    }

#if TEST_ROCKS
    [TestMethod]
    public void TestSetRocks()
    {
      var di = new DirectoryInfo("testdb");
      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          db.Put(k, v);
        }
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db.Get(k));
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }

        db.Remove(Encoding.UTF8.GetBytes("0"));
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db.Get(k));
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          if (i == 0) {
            Assert.IsFalse(db.Get(k) != null);
          }
          else {
            Assert.IsTrue(db.Get(k) != null);
          }
        }
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        db.Put(Encoding.UTF8.GetBytes("test1"), Encoding.UTF8.GetBytes("test1"));
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        db.Put(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test2"));
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        db.Put(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test3"));
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        db.Put(Encoding.UTF8.GetBytes("test3"), Encoding.UTF8.GetBytes("test4"));
      }

      using (var db = RocksDb.Open(rocksOptions, di.FullName)) {
        foreach (var i in new[] {
          new[] { "test1", "test1" }, new[] { "test2", "test3" }, new[] { "test3", "test4" }
        }) {
          Assert.IsTrue(db.Get(Encoding.UTF8.GetBytes(i[0])) != null);
        }
      }
    }
#endif

    [TestMethod]
    public void TestSetThreadUnsafe()
    {
      var opts = planeDBOptions.DisableThreadSafety();
      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, opts)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          db[k] = v;
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }

        Assert.IsTrue(db.TryRemove(Encoding.UTF8.GetBytes("0"), out var removed));
        Assert.AreEqual("00000", Encoding.UTF8.GetString(removed));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        for (var i = 1; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.IsTrue(db.ContainsKey(k));
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          if (i == 0) {
            Assert.IsFalse(db.ContainsKey(k));
            Assert.IsFalse(db.TryGetValue(k, out _));
          }
          else {
            Assert.IsTrue(db.ContainsKey(k));
            Assert.IsTrue(db.TryGetValue(k, out var v));
            Assert.AreEqual(Encoding.UTF8.GetString(v), i.ToString() + i + i + i + i);
          }
        }
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        db.Add(Encoding.UTF8.GetBytes("test1"), Encoding.UTF8.GetBytes("test1"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        db.Add(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test2"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        db.AddOrUpdate(Encoding.UTF8.GetBytes("test2"), Encoding.UTF8.GetBytes("test3"),
                       (_, __) => Encoding.UTF8.GetBytes("test3"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        db.Add(Encoding.UTF8.GetBytes("test3"), Encoding.UTF8.GetBytes("test4"));
      }

      using (var db = new PlaneDB(new DirectoryInfo("testdb"), FileMode.Open, opts)) {
        foreach (var i in new[] {
          new[] { "test1", "test1" }, new[] { "test2", "test3" }, new[] { "test3", "test4" }
        }) {
          Assert.IsTrue(db.ContainsKey(Encoding.UTF8.GetBytes(i[0])));
          Assert.IsTrue(db.TryGetValue(Encoding.UTF8.GetBytes(i[0]), out var v));
          Assert.AreEqual(i[1], Encoding.UTF8.GetString(v));
        }
      }
    }

    [TestMethod]
    public void TestTyped()
    {
      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.CreateNew, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          db[i.ToString()] = i.ToString() + i + i + i + i;
        }
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          Assert.AreEqual(db[i.ToString()], i.ToString() + i + i + i + i);
        }

        Assert.IsTrue(db.TryRemove("0", out var removed));
        Assert.AreEqual("00000", removed);
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 1; i < COUNT; ++i) {
          Assert.AreEqual(db[i.ToString()], i.ToString() + i + i + i + i);
        }
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        for (var i = 0; i < COUNT; ++i) {
          var k = i.ToString();
          if (i == 0) {
            Assert.IsFalse(db.TryGetValue(k, out _));
          }
          else {
            Assert.IsTrue(db.TryGetValue(k, out var v));
            Assert.AreEqual(v, i.ToString() + i + i + i + i);
          }
        }
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add("test1", "test1");
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add("test2", "test2");
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.AddOrUpdate("test2", "test3", (_, __) => "test3");
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        db.Add("test3", "test4");
      }

      using (var db = new StringPlaneDB(new DirectoryInfo("testdb"), FileMode.Open, planeDBOptions)) {
        foreach (var i in new[] {
          new[] { "test1", "test1" }, new[] { "test2", "test3" }, new[] { "test3", "test4" }
        }) {
          Assert.IsTrue(db.TryGetValue(i[0], out var v));
          Assert.AreEqual(i[1], v);
        }
      }
    }
  }
}
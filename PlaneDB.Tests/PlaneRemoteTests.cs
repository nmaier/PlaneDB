using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

[TestClass]
public sealed class PlaneRemoteTests
{
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
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "<Pending>")]
  public void TestRemote()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    db["1"u8.ToArray()] = "2"u8.ToArray();
    using var tokenSource = new CancellationTokenSource();
    var ro = new PlaneDBRemoteOptions("test", true) { Address = IPAddress.Loopback };
    using var remote = ro.AddDatabaseToServe("default", db).Serve(tokenSource.Token);

    _ = Assert.ThrowsException<IOException>(
      () => {
        var badcro = new PlaneDBRemoteOptions("bad auth", ro.Certificate!) {
          Port = remote.Port,
          Address = remote.Address
        };
        using var badrdb =
          (PlaneDBRemoteClient)badcro.ConnectDB("default", tokenSource.Token);
        Assert.AreEqual(0, badrdb.Count);
      });

    _ = Assert.ThrowsException<IOException>(
      () => {
        var badcro = new PlaneDBRemoteOptions("test", ro.Certificate!) {
          Port = remote.Port,
          Address = remote.Address
        };
        using var badrdb =
          (PlaneDBRemoteClient)badcro.ConnectDB("invalid", tokenSource.Token);
        Assert.AreEqual(0, badrdb.Count);
      });

    var cro = new PlaneDBRemoteOptions("test", ro.Certificate!) {
      Port = remote.Port,
      Address = remote.Address
    };
    using var rdb = (PlaneDBRemoteClient)cro.ConnectDB("default", tokenSource.Token);

    Assert.AreEqual(db.TableSpace, rdb.TableSpace);
    Assert.AreEqual(db.IsReadOnly, rdb.IsReadOnly);

    _ = Assert.ThrowsException<NotSupportedException>(() => rdb.MassRead(() => { }));
    _ = Assert.ThrowsException<NotSupportedException>(() => rdb.MassInsert(() => { }));
    _ = Assert.ThrowsException<NotSupportedException>(() => rdb.Location);
    _ = Assert.ThrowsException<NotSupportedException>(() => rdb.Options);
    _ = Assert.ThrowsException<NotSupportedException>(() => rdb.BaseDB);

    Assert.IsTrue(rdb["1"u8.ToArray()].SequenceEqual("2"u8.ToArray()));

    rdb.SetValue("1"u8.ToArray(), "3"u8.ToArray());
    Assert.IsTrue(rdb.ContainsKey("1"u8.ToArray()));
    rdb.SetValue("2"u8.ToArray(), "4"u8.ToArray());
    Assert.IsTrue(db["1"u8.ToArray()].SequenceEqual("3"u8.ToArray()));
    Assert.IsTrue(db["2"u8.ToArray()].SequenceEqual("4"u8.ToArray()));
    Assert.IsTrue(rdb["1"u8.ToArray()].SequenceEqual("3"u8.ToArray()));
    Assert.IsTrue(rdb["2"u8.ToArray()].SequenceEqual("4"u8.ToArray()));

    Assert.IsTrue(rdb.TryUpdate("1"u8.ToArray(), "5"u8.ToArray(), "3"u8.ToArray()));
    Assert.IsFalse(rdb.TryUpdate("1"u8.ToArray(), "6"u8.ToArray(), "3"u8.ToArray()));
    Assert.IsTrue(db["1"u8.ToArray()].SequenceEqual("5"u8.ToArray()));
    Assert.IsTrue(rdb["1"u8.ToArray()].SequenceEqual("5"u8.ToArray()));

    _ = Assert.ThrowsException<NotSupportedException>(rdb.Fail);

    Assert.IsTrue(rdb.Remove("1"u8.ToArray()));
    Assert.IsFalse(rdb.Remove("1"u8.ToArray()));
    Assert.IsFalse(rdb.ContainsKey("1"u8.ToArray()));
    Assert.IsTrue(rdb.TryRemove("2"u8.ToArray(), out var remValue));
    Assert.IsTrue(remValue.SequenceEqual("4"u8.ToArray()));
    Assert.IsFalse(rdb.TryRemove("2"u8.ToArray(), out remValue));

    Assert.IsTrue(rdb.TryAdd("2"u8.ToArray(), "7"u8.ToArray()));
    Assert.IsFalse(rdb.TryAdd("2"u8.ToArray(), "8"u8.ToArray()));
    Assert.IsFalse(rdb.TryAdd("2"u8.ToArray(), "8"u8.ToArray(), out var existing));
    Assert.IsTrue(existing.AsSpan().SequenceEqual("7"u8));
    Assert.IsTrue(rdb.TryAdd("22"u8.ToArray(), "8"u8.ToArray(), out existing));
    rdb.Add("33"u8.ToArray(), []);
    _ = Assert.ThrowsException<ArgumentException>(() => rdb.Add("33"u8.ToArray(), []));
    rdb.Add(new KeyValuePair<byte[], byte[]>("34"u8.ToArray(), []));
    _ = Assert.ThrowsException<ArgumentException>(
      () => rdb.Add(new KeyValuePair<byte[], byte[]>("34"u8.ToArray(), [])));

    Assert.IsTrue(db["2"u8.ToArray()].SequenceEqual("7"u8.ToArray()));
    Assert.IsTrue(rdb["2"u8.ToArray()].SequenceEqual("7"u8.ToArray()));

    var b = rdb.GetOrAdd("3"u8.ToArray(), "1"u8.ToArray());
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));

    b = rdb.GetOrAdd("3"u8.ToArray(), "2"u8.ToArray());
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));

    Assert.IsTrue(rdb.TryGetValue("3"u8.ToArray(), out existing));
    Assert.IsTrue(existing.SequenceEqual("1"u8.ToArray()));
    Assert.IsFalse(rdb.TryGetValue("does not exist"u8.ToArray(), out existing));

    b = rdb.GetOrAdd("4"u8.ToArray(), "1"u8.ToArray(), out var added);
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));
    Assert.IsTrue(added);

    rdb.Flush();

    b = rdb.GetOrAdd("4"u8.ToArray(), "2"u8.ToArray(), out added);
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));
    Assert.IsFalse(added);
    Assert.AreEqual(db.Count, rdb.Count);

    var called = false;
    b = rdb.GetOrAdd(
      "5"u8.ToArray(),
      () => {
        called = true;

        return "1"u8.ToArray();
      });
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));
    Assert.IsTrue(called);

    called = false;
    b = rdb.GetOrAdd(
      "5"u8.ToArray(),
      () => {
        called = true;

        return "2"u8.ToArray();
      });
    Assert.IsTrue(b.SequenceEqual("1"u8.ToArray()));
    Assert.IsFalse(called);

    Assert.IsTrue(
      rdb.AddOrUpdate("6"u8.ToArray(), "1"u8.ToArray(), (in byte[] _) => "2"u8.ToArray())
        .SequenceEqual("1"u8.ToArray()));
    Assert.IsTrue(
      rdb.AddOrUpdate("6"u8.ToArray(), "3"u8.ToArray(), (in byte[] _) => "4"u8.ToArray())
        .SequenceEqual("4"u8.ToArray()));

    Assert.AreEqual(8, rdb.Count);
    Assert.IsTrue(
      db.SequenceEqual(
        rdb,
        new KVComparer<byte[], byte[]>(
          PlaneByteArrayComparer.Default,
          PlaneByteArrayComparer.Default)));

    Assert.IsTrue(
      db.KeysIterator.SequenceEqual(rdb.KeysIterator, PlaneByteArrayComparer.Default));
    Assert.IsTrue(db.Keys.SequenceEqual(rdb.Keys, PlaneByteArrayComparer.Default));
    Assert.IsTrue(db.Values.SequenceEqual(rdb.Values, PlaneByteArrayComparer.Default));

    rdb.Clear();
    Assert.AreEqual(0, rdb.Count);
    Assert.AreEqual(0, db.Count);

    tokenSource.Cancel();
  }

  [TestMethod]
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "jb")]
  public void TestRemoteSet()
  {
    using var db = new PlaneSet(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    using var tokenSource = new CancellationTokenSource();
    var ro = new PlaneDBRemoteOptions("test", true) { Address = IPAddress.Loopback };
    using var remote = ro.AddDatabaseToServe("default", db).Serve(tokenSource.Token);

    var cro = new PlaneDBRemoteOptions("test", ro.Certificate!) {
      Port = remote.Port,
      Address = remote.Address
    };
    using var rdb = cro.ConnectSet("default", tokenSource.Token);

    for (var i = 0; i < 100; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      Assert.IsFalse(rdb.Contains(k));
      Assert.IsTrue(rdb.TryAdd(k));
      Assert.IsFalse(rdb.TryAdd(k));
      Assert.IsTrue(rdb.Contains(k));
    }

    tokenSource.Cancel();
  }

  [TestMethod]
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "<Pending>")]
  public void TestRemoteTyped()
  {
    using var db = new StringPlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    db["1"] = "2";
    using var tokenSource = new CancellationTokenSource();
    var ro = new PlaneDBRemoteOptions("test", true) { Address = IPAddress.Loopback };
    using var remote = ro.AddDatabaseToServe("default", db).Serve(tokenSource.Token);

    var cro = new PlaneDBRemoteOptions("test", ro.Certificate!) {
      Port = remote.Port,
      Address = remote.Address
    };
    using var rdb = cro.ConnectStringDB("default", tokenSource.Token);
    Assert.AreEqual("2", rdb["1"]);

    rdb.SetValue("1", "3");
    Assert.IsTrue(rdb.ContainsKey("1"));
    rdb.SetValue("2", "4");
    Assert.AreEqual("3", db["1"]);
    Assert.AreEqual("4", db["2"]);
    Assert.AreEqual("3", rdb["1"]);
    Assert.AreEqual("4", rdb["2"]);

    Assert.IsTrue(rdb.TryUpdate("1", "5", "3"));
    Assert.IsFalse(rdb.TryUpdate("1", "6", "3"));
    Assert.AreEqual("5", db["1"]);
    Assert.AreEqual("5", rdb["1"]);

    Assert.IsTrue(rdb.Remove("1"));
    Assert.IsFalse(rdb.Remove("1"));
    Assert.IsFalse(rdb.ContainsKey("1"));
    Assert.IsTrue(rdb.TryRemove("2", out var remValue));
    Assert.AreEqual("4", remValue);
    Assert.IsFalse(rdb.TryRemove("2", out remValue));

    Assert.IsTrue(rdb.TryAdd("2", "7"));
    Assert.IsFalse(rdb.TryAdd("2", "8"));
    Assert.IsFalse(rdb.TryAdd("2", "8", out var existing));
    Assert.AreEqual("7", db["2"]);
    Assert.AreEqual("7", rdb["2"]);
    Assert.AreEqual("7", existing);

    var b = rdb.GetOrAdd("3", "1");
    Assert.AreEqual("1", b);

    b = rdb.GetOrAdd("3", "2");
    Assert.AreEqual("1", b);

    b = rdb.GetOrAdd("4", "1", out var added);
    Assert.AreEqual("1", b);
    Assert.IsTrue(added);

    b = rdb.GetOrAdd("4", "2", out added);
    Assert.AreEqual("1", b);
    Assert.IsFalse(added);
    Assert.AreEqual(db.Count, rdb.Count);

    var called = false;
    b = rdb.GetOrAdd(
      "5",
      () => {
        called = true;

        return "1";
      });
    Assert.AreEqual("1", b);
    Assert.IsTrue(called);

    called = false;
    b = rdb.GetOrAdd(
      "5",
      () => {
        called = true;

        return "2";
      });
    Assert.AreEqual("1", b);
    Assert.IsFalse(called);

    Assert.AreEqual("1", rdb.AddOrUpdate("6", "1", (in string _) => "2"));
    Assert.AreEqual("4", rdb.AddOrUpdate("6", "3", (in string _) => "4"));

    Assert.AreEqual(5, rdb.Count);
    Assert.IsTrue(
      db.SequenceEqual(
        rdb,
        new KVComparer<string, string>(StringComparer.Ordinal, StringComparer.Ordinal)));

    rdb.Clear();
    Assert.AreEqual(0, rdb.Count);
    Assert.AreEqual(0, db.Count);

    tokenSource.Cancel();
  }

  [TestMethod]
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "jb")]
  public void TestRemoteTypedSet()
  {
    using var db = new StringPlaneSet(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    using var tokenSource = new CancellationTokenSource();
    var ro = new PlaneDBRemoteOptions("test", true) { Address = IPAddress.Loopback };
    using var remote = ro.AddDatabaseToServe("default", db).Serve(tokenSource.Token);

    var cro = new PlaneDBRemoteOptions("test", ro.Certificate!) {
      Port = remote.Port,
      Address = remote.Address
    };
    using var rdb = cro.ConnectStringSet("default", tokenSource.Token);

    for (var i = 0; i < 100; ++i) {
      var k = i.ToString();
      Assert.IsFalse(rdb.Contains(k));
      Assert.IsTrue(rdb.TryAdd(k));
      Assert.IsFalse(rdb.TryAdd(k));
      Assert.IsTrue(rdb.Contains(k));
    }

    tokenSource.Cancel();
  }
}

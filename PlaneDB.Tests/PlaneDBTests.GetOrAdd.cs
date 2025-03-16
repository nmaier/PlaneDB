using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

public sealed partial class PlaneDBTests
{
  private static void TestGetOrAddHelper<TKey>(
    IPlaneDictionary<TKey, TKey> db,
    TKey k0,
    TKey k1,
    TKey k2,
    TKey k3,
    TKey o,
    IEqualityComparer<TKey> cmp) where TKey : notnull
  {
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k0, k0), k0));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k0, k0), k0));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k0, () => o), k0));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k0, _ => o, 1), k0));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k0, k0, out var added), k0));
    Assert.IsFalse(added);

    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k1, k1, out added), k1));
    Assert.IsTrue(added);
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k1, k1, out added), k1));
    Assert.IsFalse(added);
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k1, k1, out added), k1));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k1, () => o), k1));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k1, _ => o, 1), k1));

    added = false;
    Assert.IsTrue(
      cmp.Equals(
        db.GetOrAdd(
          k2,
          () => {
            added = true;

            return k2;
          }),
        k2));
    Assert.IsTrue(added);
    added = false;
    Assert.IsTrue(
      cmp.Equals(
        db.GetOrAdd(
          k2,
          () => {
            added = true;

            return k2;
          }),
        k2));
    Assert.IsFalse(added);
    Assert.IsTrue(
      cmp.Equals(
        db.GetOrAdd(
          k2,
          delegate {
            return o;
          },
          1),
        k2));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k2, k2, out added), k2));
    Assert.IsFalse(added);
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k2, () => o), k2));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k2, k2, out added), k2));
    Assert.IsFalse(added);

    added = false;
    Assert.IsTrue(
      cmp.Equals(
        db.GetOrAdd(
          k3,
          a => {
            Assert.AreEqual(1, a);
            added = true;

            return k3;
          },
          1),
        k3));
    Assert.IsTrue(added);
    added = false;
    Assert.IsTrue(
      cmp.Equals(
        db.GetOrAdd(
          k3,
          a => {
            Assert.AreEqual(1, a);
            added = true;

            return k3;
          },
          1),
        k3));
    Assert.IsFalse(added);
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k3, k3, out added), k3));
    Assert.IsFalse(added);
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k3, () => o), k3));
    Assert.IsTrue(cmp.Equals(db.GetOrAdd(k3, k3, out added), k3));
    Assert.IsFalse(added);
  }

  [TestMethod]
  public void TestGetOrAdd()
  {
    var di = new DirectoryInfo(testDB);
    using var db = new PlaneDB(
      di,
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    TestGetOrAddHelper(
      db,
      [
        0
      ],
      [
        1
      ],
      [
        0,
        1
      ],
      "\0\0\0"u8.ToArray(),
      [],
      PlaneByteArrayComparer.Default);
  }

  [TestMethod]
  public void TestGetOrAddRange()
  {
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      foreach (var (bytes, value) in db.GetOrAddRange(
                 Enumerable.Range(0, COUNT)
                   .Where(i => i % 2 == 0)
                   .Select(
                     i => new KeyValuePair<byte[], byte[]>(
                       Encoding.UTF8.GetBytes(i.ToString()),
                       Encoding.UTF8.GetBytes(i.ToString() + i))))) {
        var key = Encoding.UTF8.GetString(bytes);
        var val = Encoding.UTF8.GetString(value);
        Assert.AreEqual(key + key, val);
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (keyBytes, valueBytes) in db.GetOrAddRange(
                 Enumerable.Range(0, COUNT)
                   .Select(i => Encoding.UTF8.GetBytes(i.ToString())),
                 (in byte[] bytes) =>
                   Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(bytes) + "i"))) {
        var key = Encoding.UTF8.GetString(keyBytes);
        var i = int.Parse(key);
        var val = Encoding.UTF8.GetString(valueBytes);
        if (i % 2 == 0) {
          Assert.AreEqual(key + key, val);
        }
        else {
          Assert.AreEqual(key + "i", val);
        }
      }
    }

    const int V0 = -1;
    using (var db = new PlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (keyBytes, valueBytes) in db.GetOrAddRange(
                 Enumerable.Range(10, COUNT)
                   .Select(i => Encoding.UTF8.GetBytes(i.ToString())),
                 Encoding.UTF8.GetBytes(V0.ToString()))) {
        var key = Encoding.UTF8.GetString(keyBytes);
        var i = int.Parse(key);
        var val = Encoding.UTF8.GetString(valueBytes);
        if (i >= COUNT) {
          Assert.AreEqual(val, V0.ToString(), i.ToString());
        }
        else if (i % 2 == 0) {
          Assert.AreEqual(key + key, val, i.ToString());
        }
        else {
          Assert.AreEqual(key + "i", val, i.ToString());
        }
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (keyBytes, valueBytes) in db.GetOrAddRange(
                 Enumerable.Range(100, COUNT)
                   .Select(i => Encoding.UTF8.GetBytes(i.ToString())),
                 (in byte[] _, int a) => {
                   Assert.AreEqual(1, a);

                   return Encoding.UTF8.GetBytes(V0.ToString());
                 },
                 1)) {
        var key = Encoding.UTF8.GetString(keyBytes);
        var i = int.Parse(key);
        var val = Encoding.UTF8.GetString(valueBytes);
        if (i >= COUNT) {
          Assert.AreEqual(val, V0.ToString(), i.ToString());
        }
        else if (i % 2 == 0) {
          Assert.AreEqual(key + key, val, i.ToString());
        }
        else {
          Assert.AreEqual(key + "i", val, i.ToString());
        }
      }
    }
  }

  [TestMethod]
  public void TestGetOrAddRangeTyped()
  {
    using (var db = new StringPlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      foreach (var (key, value) in db.GetOrAddRange(
                 Enumerable.Range(0, COUNT)
                   .Where(i => i % 2 == 0)
                   .Select(
                     i => new KeyValuePair<string, string>(
                       i.ToString(),
                       i.ToString() + i)))) {
        Assert.AreEqual(key + key, value);
      }
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (key, value) in db.GetOrAddRange(
                 Enumerable.Range(0, COUNT).Select(i => i.ToString()),
                 (in string bytes) => bytes + "i")) {
        var i = int.Parse(key);
        if (i % 2 == 0) {
          Assert.AreEqual(key + key, value);
        }
        else {
          Assert.AreEqual(key + "i", value);
        }
      }
    }

    const int V0 = -1;
    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (key, value) in db.GetOrAddRange(
                 Enumerable.Range(10, COUNT).Select(i => i.ToString()),
                 V0.ToString())) {
        var i = int.Parse(key);
        if (i >= COUNT) {
          Assert.AreEqual(value, V0.ToString(), i.ToString());
        }
        else if (i % 2 == 0) {
          Assert.AreEqual(key + key, value, i.ToString());
        }
        else {
          Assert.AreEqual(key + "i", value, i.ToString());
        }
      }
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var (key, value) in db.GetOrAddRange(
                 Enumerable.Range(100, COUNT).Select(i => i.ToString()),
                 (in string _, int a) => {
                   Assert.AreEqual(1, a);

                   return V0.ToString();
                 },
                 1)) {
        var i = int.Parse(key);
        if (i >= COUNT) {
          Assert.AreEqual(value, V0.ToString(), i.ToString());
        }
        else if (i % 2 == 0) {
          Assert.AreEqual(key + key, value, i.ToString());
        }
        else {
          Assert.AreEqual(key + "i", value, i.ToString());
        }
      }
    }
  }

  [TestMethod]
  public void TestGetOrAddTyped()
  {
    var di = new DirectoryInfo(testDB);
    using var db = new StringPlaneDB(
      di,
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    TestGetOrAddHelper(db, "1", "a", "bb", "cc", string.Empty, StringComparer.Ordinal);
  }
}

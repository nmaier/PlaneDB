using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

public sealed partial class PlaneDBTests
{
  public enum TestTryAddMultipleType
  {
    TryAdd,
    TryAddSame,
    CopyTo,
    CopyToSame,
    TryAddSameMemory,
    CopyToSameMemory
  }

  [TestMethod]
  public void TestAddOrUpdate()
  {
    var di = new DirectoryInfo(testDB);
    var v1 = new byte[10];
    v1[1] = 1;
    var v2 = new byte[10];
    v2[2] = 1;

    using (var db = new PlaneDB(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var k = new byte[10];

      Assert.IsTrue(
        db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v1));
      Assert.IsTrue(
        db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

      k[1] = 1;
      Assert.IsTrue(
        db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v1));
      Assert.IsTrue(
        db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

      k[2] = 1;
      Assert.IsTrue(
        db.AddOrUpdate(
            k,
            a => {
              Assert.AreEqual(1, a);

              return v1;
            },
            (in byte[] _, int a) => {
              Assert.AreEqual(2, a);

              return v2;
            },
            1)
          .AsSpan()
          .SequenceEqual(v1));

      Assert.IsTrue(
        db.AddOrUpdate(
            k,
            a => {
              Assert.AreEqual(1, a);

              return v1;
            },
            (in byte[] _, int a) => {
              Assert.AreEqual(2, a);

              return v2;
            },
            2)
          .AsSpan()
          .SequenceEqual(v2));
    }

    using (var db = new PlaneDB(di, planeOptions)) {
      var k = new byte[10];

      Assert.IsTrue(
        db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

      k[1] = 1;
      Assert.IsTrue(
        db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

      k[2] = 1;
      Assert.IsTrue(
        db.AddOrUpdate(
            k,
            a => {
              Assert.AreEqual(1, a);

              return v1;
            },
            (in byte[] _, int a) => {
              Assert.AreEqual(2, a);

              return v2;
            },
            2)
          .AsSpan()
          .SequenceEqual(v2));
    }

    using (var db = new PlaneDB(di, planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      var k = new byte[10];

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => _ = db.AddOrUpdate(k, v1, (in byte[] _) => v2));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => _ = db.AddOrUpdate(
          k,
          a => {
            Assert.AreEqual(1, a);

            return v1;
          },
          (in byte[] _, int a) => {
            Assert.AreEqual(2, a);

            return v2;
          },
          2));
      // ReSharper restore AccessToDisposedClosure
    }
  }

  [TestMethod]
  public void TestTryAdd()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(db.TryAdd(k, v));
    }
  }

  [TestMethod]
  [DataRow(TestTryAddMultipleType.TryAdd)]
  [DataRow(TestTryAddMultipleType.TryAddSame)]
  [DataRow(TestTryAddMultipleType.TryAddSameMemory)]
  [DataRow(TestTryAddMultipleType.CopyTo)]
  [DataRow(TestTryAddMultipleType.CopyToSame)]
  [DataRow(TestTryAddMultipleType.CopyToSameMemory)]
  public void TestTryAddMultiple(TestTryAddMultipleType testType)
  {
    using var db = new StringPlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite).DisableThreadSafety());

    using var db2 = testType switch {
      TestTryAddMultipleType.TryAdd => (IPlaneDictionary<string, string>)new
        TypedPlaneDB<string, string>(
          new StupidStringSerializer("key"),
          new StupidStringSerializer("value"),
          new DirectoryInfo(testDB),
          planeOptions.WithOpenMode(PlaneOpenMode.ReadWrite)
            .UsingTablespace(testType.ToString())),
      TestTryAddMultipleType.TryAddSame => new StringPlaneDB(
        new DirectoryInfo(testDB),
        planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite)
          .UsingTablespace(testType.ToString())),
      TestTryAddMultipleType.TryAddSameMemory =>
        new PlaneMemoryDictionary<string, string>(StringComparer.Ordinal),
      TestTryAddMultipleType.CopyTo => new TypedPlaneDB<string, string>(
        new StupidStringSerializer("key"),
        new StupidStringSerializer("value"),
        new DirectoryInfo(testDB),
        planeOptions.WithOpenMode(PlaneOpenMode.ReadWrite)
          .UsingTablespace(testType.ToString())),
      TestTryAddMultipleType.CopyToSame => new StringPlaneDB(
        new DirectoryInfo(testDB),
        planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite)
          .UsingTablespace(testType.ToString())),
      TestTryAddMultipleType.CopyToSameMemory =>
        new PlaneMemoryDictionary<string, string>(StringComparer.Ordinal),
      _ => throw new ArgumentOutOfRangeException(nameof(testType), testType, null)
    };

    var db3 = db2 as IPlaneDB<string, string>;

    Assert.AreEqual(db.IsReadOnly, db2.IsReadOnly);
    Assert.AreEqual(0, db.CurrentBloomBits);
    Assert.AreEqual(0, db.CurrentIndexBlockCount);
    Assert.AreEqual(0, db.CurrentRealSize);
    Assert.AreEqual(0, db.CurrentTableCount);
    if (db3 != null) {
      Assert.AreEqual(db3.Location.FullName, db.Location.FullName);
      Assert.AreNotEqual(db.TableSpace, db3.TableSpace);
      Assert.AreNotEqual(db.Options, db3.Options);
      Assert.AreEqual(0, db3.CurrentBloomBits);
      Assert.AreEqual(0, db3.CurrentIndexBlockCount);
      Assert.AreEqual(0, db3.CurrentRealSize);
      Assert.AreEqual(0, db3.CurrentTableCount);
    }

    // Simple
    var (added, ignored) = db.TryAdd(
    [
      new KeyValuePair<string, string>("a", "1")
    ]);
    Assert.AreEqual(1, added);
    Assert.AreEqual(0, ignored);

    // Ignored
    (added, ignored) = db.TryAdd(
    [
      new KeyValuePair<string, string>("a", "1")
    ]);
    Assert.AreEqual(0, added);
    Assert.AreEqual(1, ignored);

    // Simple with some ignored
    (added, ignored) = db.TryAdd(
    [
      new KeyValuePair<string, string>("b", "2"),
      new KeyValuePair<string, string>("a", "1")
    ]);
    Assert.AreEqual(1, added);
    Assert.AreEqual(1, ignored);

    // collection with some ignored
    (added, ignored) = db.TryAdd(
    [
      new KeyValuePair<string, string>("b", "2"),
      new KeyValuePair<string, string>("a", "1"),
      new KeyValuePair<string, string>("d", "4"),
      new KeyValuePair<string, string>("c", "3")
    ]);
    Assert.AreEqual(2, added);
    Assert.AreEqual(2, ignored);

    // enumerable
    (added, ignored) = db.TryAdd(
      Enumerable.Range(0, 10)
        .Select(i => new KeyValuePair<string, string>(i.ToString(), i.ToString())));
    Assert.AreEqual(10, added);
    Assert.AreEqual(0, ignored);

    // enumerable some ignored
    (added, ignored) = db.TryAdd(
      Enumerable.Range(2, 10)
        .Select(i => new KeyValuePair<string, string>(i.ToString(), i.ToString())));
    Assert.AreEqual(2, added);
    Assert.AreEqual(8, ignored);

    // add to self
    (added, ignored) = db.TryAdd(db);
    Assert.AreEqual(0, added);
    Assert.AreEqual(16, ignored);

    // remove
    Assert.AreEqual(
      1,
      db.TryRemove(
      [
        "a"
      ]));
    Assert.AreEqual(
      1,
      db.TryRemove(
      [
        "a",
        "b"
      ]));
    Assert.AreEqual(
      0,
      db.TryRemove(
      [
        "a",
        "b"
      ]));

    switch (testType) {
      case TestTryAddMultipleType.TryAdd:
      case TestTryAddMultipleType.TryAddSame:
      case TestTryAddMultipleType.TryAddSameMemory:
        (added, ignored) = db2.TryAdd(db);
        Assert.AreEqual(db.Count, added);
        Assert.AreEqual(0, ignored);

        break;
      case TestTryAddMultipleType.CopyTo:
      case TestTryAddMultipleType.CopyToSame:
      case TestTryAddMultipleType.CopyToSameMemory:
        db.CopyTo(db2);
        Assert.AreEqual(db.Count, db2.Count);

        break;
      default:
        throw new ArgumentOutOfRangeException(nameof(testType), testType, null);
    }

    (added, ignored) = db2.TryAdd(db);
    Assert.AreEqual(db.Count, ignored);
    Assert.AreEqual(0, added);

    Assert.IsTrue(db.ContainsKey("c"));
    Assert.IsTrue(db2.ContainsKey("c"));
    Assert.IsTrue(db.Contains(new KeyValuePair<string, string>("c", "c")));
    Assert.IsTrue(db2.Contains(new KeyValuePair<string, string>("c", "3")));
    Assert.IsTrue(db.Remove("c"));
    db.Add(new KeyValuePair<string, string>("c", "3"));
    db.Flush();
    if (db3 != null) {
      db3.Flush();
      Assert.AreNotEqual(db.CurrentDiskSize, db3.CurrentDiskSize);
    }

    Assert.IsTrue(
      db2.KeysIterator.OrderBy(s => s, StringComparer.Ordinal)
        .SequenceEqual(db.KeysIterator.OrderBy(s => s, StringComparer.Ordinal)));
    Assert.IsTrue(
      db2.Values.OrderBy(s => s, StringComparer.Ordinal)
        .SequenceEqual(db.Values.OrderBy(s => s, StringComparer.Ordinal)));
    Assert.AreEqual(db.Count, db2.Count);
    Assert.AreNotEqual(0, db.CurrentBloomBits);
    Assert.AreNotEqual(0, db.CurrentIndexBlockCount);
    Assert.AreNotEqual(0, db.CurrentRealSize);
    Assert.AreNotEqual(0, db.CurrentTableCount);

    if (db3 != null) {
      Assert.AreNotEqual(0, db3.CurrentBloomBits);
      Assert.AreNotEqual(0, db3.CurrentIndexBlockCount);
      Assert.AreNotEqual(0, db3.CurrentRealSize);
      Assert.AreNotEqual(0, db3.CurrentTableCount);
    }

    Assert.IsTrue(db2.Remove(new KeyValuePair<string, string>("c", "nope")));
    Assert.AreEqual(14, db.TryRemove(db.Keys));
    Assert.AreEqual(13, db2.TryRemove(db2.Keys));
    Assert.AreEqual(0, db.Count);
    Assert.AreEqual(0, db2.Count);
  }

  [TestMethod]
  public void TestTryAddOut()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
      Assert.IsTrue(db.TryAdd(k, v, out _));
      Assert.IsFalse(db.TryAdd(k, v, out var existing));
      Assert.IsTrue(existing.AsSpan().SequenceEqual(v), "values match");
    }
  }

  [TestMethod]
  public void TestTryAddOutTyped()
  {
    using var db = new StringPlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    for (var i = 0; i < COUNT; ++i) {
      var k = i.ToString();
      var v = i.ToString() + i + i + i + i;
      Assert.IsTrue(db.TryAdd(k, v, out _));
      Assert.IsFalse(db.TryAdd(k, v, out var existing));
      Assert.AreEqual(existing, v, "values match");
    }
  }

  private sealed class StupidStringSerializer(string prefix) : IPlaneSerializer<string>
  {
    public string Deserialize(ReadOnlySpan<byte> bytes)
    {
      return PlaneStringSerializer.Default.Deserialize(bytes)[prefix.Length..];
    }

    public byte[] Serialize(in string obj)
    {
      return PlaneStringSerializer.Default.Serialize(prefix + obj);
    }
  }
}

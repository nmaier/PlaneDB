using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

public sealed partial class PlaneDBTests
{
  [TestMethod]
  public void TestAddOrUpdateMemory()
  {
    var v1 = new byte[10];
    v1[1] = 1;
    var v2 = new byte[10];
    v2[2] = 1;

    using var db =
      new PlaneMemoryDictionary<byte[], byte[]>(PlaneByteArrayComparer.Default);
    var k = new byte[10];

    Assert.IsTrue(db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v1));
    Assert.IsTrue(db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

    k = new byte[10];
    k[1] = 1;
    Assert.IsTrue(
      db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v1));
    Assert.IsTrue(
      db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

    k = new byte[10];
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

    k = new byte[10];

    Assert.IsTrue(db.AddOrUpdate(k, v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

    k = new byte[10];
    k[1] = 1;
    Assert.IsTrue(
      db.AddOrUpdate(k, () => v1, (in byte[] _) => v2).AsSpan().SequenceEqual(v2));

    k = new byte[10];
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

  [TestMethod]
  public async Task TestBasicIteratorsAsyncMemory()
  {
    using var db =
      new PlaneMemoryDictionary<byte[], byte[]>(PlaneByteArrayComparer.Default);
    var value = new byte[100];
    for (var i = 0; i < 10000; ++i) {
      Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
    }

    Assert.AreEqual(10000, db.Count);

    // Read twice
    // Be a little awkward here, to avoid dealing with net6 limitations
    List<KeyValuePair<byte[], byte[]>> kvs = [];
    await foreach (var kv in EnumerateEnumerator()) {
      kvs.Add(kv);
    }

    var read = kvs.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();
    Assert.AreEqual(db.Count, read);

    kvs.Clear();
    await foreach (var kv in EnumerateEnumerator()) {
      kvs.Add(kv);
    }

    read = kvs.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();
    Assert.AreEqual(db.Count, read);

    List<byte[]> keys = [];
    await foreach (var key in db.GetKeysIteratorAsync(CancellationToken.None)) {
      keys.Add(key);
    }

    read = keys.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();
    Assert.AreEqual(db.Count, read);
    Assert.IsTrue(db.Keys.SequenceEqual(keys));

    return;

    async IAsyncEnumerable<KeyValuePair<byte[], byte[]>> EnumerateEnumerator()
    {
      // ReSharper disable once AccessToDisposedClosure
      await using var asyncEnumerator = db.GetAsyncEnumerator();
      while (await asyncEnumerator.MoveNextAsync()) {
        yield return asyncEnumerator.Current;
      }
    }
  }

  [TestMethod]
  public void TestGetOrAddMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>(StringComparer.Ordinal);
    TestGetOrAddHelper(db, "1", "a", "bb", "cc", string.Empty, StringComparer.Ordinal);
  }

  [TestMethod]
  public void TestGetOrAddRangeMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>(StringComparer.Ordinal);
    foreach (var (key, value) in db.GetOrAddRange(
               Enumerable.Range(0, COUNT)
                 .Where(i => i % 2 == 0)
                 .Select(
                   i => new KeyValuePair<string, string>(
                     i.ToString(),
                     i.ToString() + i)))) {
      Assert.AreEqual(key + key, value);
    }

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

    const int V0 = -1;
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

  [TestMethod]
  public void TestMemoryDict()
  {
    using var db =
      new PlaneMemoryDictionary<byte[], byte[]>(PlaneByteArrayComparer.Default);
    db[[]] = [];
    using var db2 =
      new PlaneMemoryDictionary<byte[], byte[]>(PlaneByteArrayComparer.Default);
    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
      if (i % 2 == 0) {
        db[k] = v;
      }
      else {
        db.SetValue(k, v);
      }
    }

    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetString(db[k]);
      Assert.AreEqual(v, i.ToString() + i + i + i + i);
    }

    db.CopyTo(db2);
    Assert.AreEqual(db2.Count, db.Count);

    Assert.IsTrue(db.TryRemove("0"u8.ToArray(), out var removed));
    Assert.AreEqual("00000", Encoding.UTF8.GetString(removed));

    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetString(db2[k]);
      Assert.AreEqual(v, i.ToString() + i + i + i + i);
    }

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

    _ = db.TryAdd("test1"u8.ToArray(), "test1"u8.ToArray());
    _ = db.TryAdd("test2"u8.ToArray(), "test2"u8.ToArray());

    _ = db.AddOrUpdate(
      "test2"u8.ToArray(),
      "test3"u8.ToArray(),
      (in byte[] _) => "test3"u8.ToArray());

    _ = db.TryAdd("test3"u8.ToArray(), "test4"u8.ToArray());

    foreach (var i in new[] { [
                 "test1",
                 "test1"
               ], [
                 "test2",
                 "test3"
               ],
               new[] { "test3", "test4" }
             }) {
      Assert.IsTrue(db.ContainsKey(Encoding.UTF8.GetBytes(i[0])));
      Assert.IsTrue(db.TryGetValue(Encoding.UTF8.GetBytes(i[0]), out var v));
      Assert.AreEqual(i[1], Encoding.UTF8.GetString(v));
    }

    using var db3 = new PlaneMemoryDictionary<string, string>(
    [
      new KeyValuePair<string, string>("1", "0")
    ]);
    Assert.AreEqual(1, db3.Count);
    Assert.IsTrue(db3.ContainsKey("1"));

    using var db4 = new PlaneMemoryDictionary<string, string>(
      [
        new KeyValuePair<string, string>("2", "0")
      ],
      StringComparer.Ordinal);
    Assert.AreEqual(1, db4.Count);
    Assert.IsTrue(db4.ContainsKey("2"));

    _ = Assert.ThrowsExactly<ArgumentException>(
      () => {
        // ReSharper disable once CollectionNeverQueried.Local
        using var db5 = new PlaneMemoryDictionary<string, string>(
          [
            new KeyValuePair<string, string>("2", "0"),
            new KeyValuePair<string, string>("2", "1")
          ],
          StringComparer.Ordinal);
      });

    _ = Assert.ThrowsExactly<ArgumentException>(
      () => {
        // ReSharper disable once CollectionNeverQueried.Local
        using var db6 = new PlaneMemoryDictionary<string, string>(
          [
            new KeyValuePair<string, string>("a", "0"),
            new KeyValuePair<string, string>("A", "1")
          ],
          StringComparer.OrdinalIgnoreCase);
      });
  }

  [TestMethod]
  public void TestTryAddOutMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>(StringComparer.Ordinal);
    for (var i = 0; i < COUNT; ++i) {
      var k = i.ToString();
      var v = i.ToString() + i + i + i + i;
      Assert.IsTrue(db.TryAdd(k, v, out _));
      Assert.IsFalse(db.TryAdd(k, v, out var existing));
      Assert.AreEqual(existing, v, "values match");
    }
  }

  [TestMethod]
  public void TestTryUpdateFactoryArgMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>();
    TestTryUpdateFactoryArgTypedHelper(
      db,
      string.Empty,
      StringComparer.Ordinal,
      i => (i.ToString(), i.ToString() + i + i + i));
  }

  [TestMethod]
  public void TestTryUpdateFactoryMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>();
    for (var i = 0; i < COUNT; ++i) {
      var k = i.ToString();
      var v = i.ToString() + i + i + i + i;
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in string _, in string _, [MaybeNullWhen(false)] out string _) =>
            throw new ArgumentException()));
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in string _, in string _, [MaybeNullWhen(false)] out string o) => {
            o = string.Empty;

            return false;
          }));
      Assert.IsTrue(
        db.TryUpdate(
          k,
          (in string _, in string _, [MaybeNullWhen(false)] out string o) => {
            o = string.Empty;

            return true;
          }));
      Assert.AreEqual(0, db[k].Length);
    }
  }

  [TestMethod]
  public void TestTryUpdateMemory()
  {
    using var db = new PlaneMemoryDictionary<string, string>();
    for (var i = 0; i < COUNT; ++i) {
      var k = i.ToString();
      var v = i.ToString() + i + i + i + i;
      Assert.IsFalse(db.TryUpdate(k, v, string.Empty));
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(db.TryUpdate(k, v, string.Empty));
      Assert.IsTrue(db.TryUpdate(k, string.Empty, v));
      Assert.AreEqual(0, db[k].Length);
    }
  }
}

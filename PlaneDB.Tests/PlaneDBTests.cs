using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using NMaier.BlockStream.Transformers;

// ReSharper disable CollectionNeverQueried.Local
namespace NMaier.PlaneDB.Tests;

[TestClass]
public sealed partial class PlaneDBTests
{
  private const int COUNT = 5_000;

  private static readonly PlaneOptions planeOptions =
    new PlaneOptions(PlaneOpenMode.ExistingReadWrite).WithCompression();

  private static readonly string testDB = $"{Path.GetTempPath()}/PlaneTestDB";

  private static void TestTryUpdateFactoryArgTypedHelper<TKey, TValue>(
    IPlaneDictionary<TKey, TValue> db,
    TValue replacement,
    IEqualityComparer<TValue> comparer,
    Func<int, (TKey, TValue)> generator) where TKey : notnull
  {
    for (var i = 0; i < COUNT; ++i) {
      var (k, v) = generator(i);
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in TValue _, int _, [MaybeNullWhen(false)] out TValue _) =>
            throw new InvalidOperationException(),
          1));
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in TValue _, int a, [MaybeNullWhen(false)] out TValue o) => {
            Assert.AreEqual(2, a);
            o = replacement;

            return false;
          },
          2));
      Assert.IsTrue(
        db.TryUpdate(
          k,
          (in TValue _, int a, [MaybeNullWhen(false)] out TValue o) => {
            Assert.AreEqual(3, a);
            o = replacement;

            return true;
          },
          3));
      Assert.IsTrue(comparer.Equals(db[k], replacement));
    }
  }

  [TestInitialize]
  public void Initialize()
  {
    var di = new DirectoryInfo(testDB);
    if (di.Exists) {
      di.Delete(true);
    }
  }

  [TestMethod]
  public void TestBasicIterators()
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.DisableJournal()
      .WithKeyCacheMode(PlaneKeyCacheMode.NoKeyCaching);
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      var value = new byte[100];
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }

      Assert.AreEqual(10000, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      db.Compact();
      Assert.AreEqual(10000, db.Count);
    }

    int read;
    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(10000, db.Count);
      using var enumerator = db.GetEnumerator();

      IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateEnumerator()
      {
        // ReSharper disable AccessToDisposedClosure
        enumerator.Reset();
        while (enumerator.MoveNext()) {
          yield return enumerator.Current;
        }
        // ReSharper restore AccessToDisposedClosure
      }

      // Read twice
      read = EnumerateEnumerator()
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);

      read = EnumerateEnumerator()
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsFalse(db.IsReadOnly);
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.IsTrue(
        db.TryAdd(BitConverter.GetBytes(1000), BitConverter.GetBytes(100000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out var removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(100000)));
      db.Add(
        new KeyValuePair<byte[], byte[]>(
          BitConverter.GetBytes(1000),
          BitConverter.GetBytes(200000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(200000)));
      read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

      Assert.AreEqual(db.Count, read);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(BitConverter.GetBytes(0), []));

      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(new KeyValuePair<byte[], byte[]>(BitConverter.GetBytes(0), [])));

      _ = Assert.ThrowsExactly<KeyNotFoundException>(
        () => _ = db[[]].Length); // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.ExistingReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.KeysIterator.Select((e, i) => new KeyValuePair<byte[], int>(e, i))
        .Count();

      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.Keys.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      Assert.AreEqual(db.Count, read);
      var arr = new KeyValuePair<byte[], byte[]>[db.Count + 100];
      db.CopyTo(arr, 0);
      db.CopyTo(arr, 100);
      Assert.IsTrue(arr[0].Value.AsSpan().SequenceEqual(arr[100].Value));
      Assert.IsTrue(arr[0].Key.AsSpan().SequenceEqual(arr[100].Key));

      db.CopyTo(arr, 0);
      Assert.IsTrue(arr[db.Count - 1].Value.AsSpan().SequenceEqual(arr.Last().Value));
      Assert.IsTrue(arr[db.Count - 1].Key.AsSpan().SequenceEqual(arr.Last().Key));

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<IndexOutOfRangeException>(() => db.CopyTo(arr, 1000));
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsTrue(db.IsReadOnly);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.SetValue(
          [
            byte.MinValue
          ],
          [
            byte.MaxValue
          ]));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], [], out _));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryUpdate([], [], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, int _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          },
          1));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(db.Clear);
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      db.MassInsert(() => { });
      Assert.AreEqual(1, db.MassInsert(() => 1));

      db.Clear();
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => db.MassInsert(() => { }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => _ = db.MassInsert(() => 1));
    }
  }

  [TestMethod]
  public async Task TestBasicIteratorsAsync()
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.DisableJournal()
      .WithKeyCacheMode(PlaneKeyCacheMode.NoKeyCaching);
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      var value = new byte[100];
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }

      Assert.AreEqual(10000, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      db.Compact();
      Assert.AreEqual(10000, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(10000, db.Count);

      async IAsyncEnumerable<KeyValuePair<byte[], byte[]>> EnumerateEnumerator()
      {
        // ReSharper disable once AccessToDisposedClosure
        await using var asyncEnumerator = db.GetAsyncEnumerator();
        while (await asyncEnumerator.MoveNextAsync()) {
          yield return asyncEnumerator.Current;
        }
      }

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
    }
  }

  [TestMethod]
  public void TestBasicIteratorsWithEncryption()
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.DisableJournal()
      .WithKeyCacheMode(PlaneKeyCacheMode.NoKeyCaching)
      .WithEncryption("testing");
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      var value = new byte[100];
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }

      Assert.AreEqual(10000, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      db.Compact();
      Assert.AreEqual(10000, db.Count);
    }

    int read;
    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(10000, db.Count);
      using var enumerator = db.GetEnumerator();

      IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateEnumerator()
      {
        // ReSharper disable AccessToDisposedClosure
        enumerator.Reset();
        while (enumerator.MoveNext()) {
          yield return enumerator.Current;
        }
        // ReSharper restore AccessToDisposedClosure
      }

      // Read twice
      read = EnumerateEnumerator()
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);

      read = EnumerateEnumerator()
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsFalse(db.IsReadOnly);
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.IsTrue(
        db.TryAdd(BitConverter.GetBytes(1000), BitConverter.GetBytes(100000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out var removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(100000)));
      db.Add(
        new KeyValuePair<byte[], byte[]>(
          BitConverter.GetBytes(1000),
          BitConverter.GetBytes(200000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(200000)));
      read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

      Assert.AreEqual(db.Count, read);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(BitConverter.GetBytes(0), []));

      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(new KeyValuePair<byte[], byte[]>(BitConverter.GetBytes(0), [])));

      _ = Assert.ThrowsExactly<KeyNotFoundException>(() => _ = db[[]].Length);
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.ExistingReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.KeysIterator.Select((e, i) => new KeyValuePair<byte[], int>(e, i))
        .Count();

      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.Keys.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      Assert.AreEqual(db.Count, read);
      var arr = new KeyValuePair<byte[], byte[]>[db.Count + 100];
      db.CopyTo(arr, 0);
      db.CopyTo(arr, 100);
      Assert.IsTrue(arr[0].Value.AsSpan().SequenceEqual(arr[100].Value));
      Assert.IsTrue(arr[0].Key.AsSpan().SequenceEqual(arr[100].Key));

      db.CopyTo(arr, 0);
      Assert.IsTrue(arr[db.Count - 1].Value.AsSpan().SequenceEqual(arr.Last().Value));
      Assert.IsTrue(arr[db.Count - 1].Key.AsSpan().SequenceEqual(arr.Last().Key));

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<IndexOutOfRangeException>(() => db.CopyTo(arr, 1000));
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsTrue(db.IsReadOnly);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.SetValue(
          [
            byte.MinValue
          ],
          [
            byte.MaxValue
          ]));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], [], out _));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryUpdate([], [], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, int _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          },
          1));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(db.Clear);
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      db.MassInsert(() => { });
      Assert.AreEqual(1, db.MassInsert(() => 1));

      db.Clear();
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => db.MassInsert(() => { }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => _ = db.MassInsert(() => 1));
    }
  }

  [TestMethod]
  public void TestBasicIteratorsWithSalt()
  {
    var r = new byte[Constants.SALT_BYTES];
    RandomNumberGenerator.Fill(r);
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.DisableJournal()
      .WithKeyCacheMode(PlaneKeyCacheMode.NoKeyCaching)
      .WithBlockTransformer(new XORSaltableTransformer(r));
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      var value = new byte[100];
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }

      Assert.AreEqual(10000, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      db.Compact();
      Assert.AreEqual(10000, db.Count);
    }

    int read;
    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(10000, db.Count);
      using var enumerator = db.GetEnumerator();

      // Read twice
      read = EnumerateEnumerator(enumerator)
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);

      read = EnumerateEnumerator(enumerator)
        .Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i))
        .Count();
      Assert.AreEqual(db.Count, read);

      static IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateEnumerator(
        IEnumerator<KeyValuePair<byte[], byte[]>> enumerator)
      {
        enumerator.Reset();
        while (enumerator.MoveNext()) {
          yield return enumerator.Current;
        }
      }
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsFalse(db.IsReadOnly);
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.IsTrue(
        db.TryAdd(BitConverter.GetBytes(1000), BitConverter.GetBytes(100000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out var removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(100000)));
      db.Add(
        new KeyValuePair<byte[], byte[]>(
          BitConverter.GetBytes(1000),
          BitConverter.GetBytes(200000)));
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out removed));
      Assert.AreEqual(9999, db.Count);
      Assert.IsTrue(removed.AsSpan().SequenceEqual(BitConverter.GetBytes(200000)));
      read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

      Assert.AreEqual(db.Count, read);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(BitConverter.GetBytes(0), []));

      _ = Assert.ThrowsExactly<ArgumentException>(
        () => db.Add(new KeyValuePair<byte[], byte[]>(BitConverter.GetBytes(0), [])));

      _ = Assert.ThrowsExactly<KeyNotFoundException>(
        () => {
          _ = db[[]].Length;
        });
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.ExistingReadWrite))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.KeysIterator.Select((e, i) => new KeyValuePair<byte[], int>(e, i))
        .Count();

      Assert.AreEqual(db.Count, read);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      read = db.Keys.Select((e, i) => new KeyValuePair<byte[], int>(e, i)).Count();

      Assert.AreEqual(db.Count, read);
      var arr = new KeyValuePair<byte[], byte[]>[db.Count + 100];
      db.CopyTo(arr, 0);
      db.CopyTo(arr, 100);
      Assert.IsTrue(arr[0].Value.AsSpan().SequenceEqual(arr[100].Value));
      Assert.IsTrue(arr[0].Key.AsSpan().SequenceEqual(arr[100].Key));

      db.CopyTo(arr, 0);
      Assert.IsTrue(arr[db.Count - 1].Value.AsSpan().SequenceEqual(arr.Last().Value));
      Assert.IsTrue(arr[db.Count - 1].Key.AsSpan().SequenceEqual(arr.Last().Key));

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<IndexOutOfRangeException>(() => db.CopyTo(arr, 1000));
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.IsTrue(db.IsReadOnly);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      // ReSharper disable AccessToDisposedClosure
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.SetValue(
          [
            byte.MinValue
          ],
          [
            byte.MaxValue
          ]));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryAdd([], [], out _));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryUpdate([], [], []));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        () => db.TryUpdate(
          [],
          (in byte[] _, int _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          },
          1));

      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(db.Clear);
      // ReSharper restore AccessToDisposedClosure
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(db.Count, read);
      Assert.IsTrue(db.AllLevels.Count > 0);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      db.MassInsert(() => { });
      Assert.AreEqual(1, db.MassInsert(() => 1));

      db.Clear();
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);
    }

    using (var db = new PlaneDB(di, dbOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual("default", db.TableSpace);
      Assert.AreEqual(0, db.Count);
      Assert.AreEqual(0, db.AllLevels.Count);
      Assert.AreEqual(0, db.CurrentDiskSize);
      Assert.AreEqual(0, db.CurrentBloomBits);
      Assert.AreEqual(0, db.CurrentIndexBlockCount);

      db.MassRead(() => { });
      Assert.AreEqual(1, db.MassRead(() => 1));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => db.MassInsert(() => { }));
      _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
        // ReSharper disable once AccessToDisposedClosure
        () => _ = db.MassInsert(() => 1));
    }
  }

  [TestMethod]
  public void TestBasicRange()
  {
    var di = new DirectoryInfo(testDB);
    using (var db = new PlaneDB(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 100; ++i) {
        var kv = new[] { (byte)i };
        db.SetValue(kv, kv);
      }

      Assert.AreEqual(
        100,
        db.Range(
            [
              0
            ],
            [
              99
            ])
          .Count());
      Assert.AreEqual(
        50,
        db.Range(
            [
              50
            ],
            [
              200
            ])
          .Count());

      Assert.AreEqual(
        50,
        db.Range(
            [
              1
            ],
            [
              50
            ])
          .Count());

      Assert.AreEqual(
        0,
        db.Range(
            [
              50
            ],
            [
              40
            ])
          .Count());

      Assert.AreEqual(
        0,
        db.Range(
            [
              200
            ],
            [
              210
            ])
          .Count());

      Assert.AreEqual(
        1,
        db.Range(
            [
              99
            ],
            [
              99
            ])
          .Count());
      Assert.AreEqual(
        1,
        db.Range(
            [
              0
            ],
            [
              0
            ])
          .Count());
      Assert.AreEqual(
        0,
        db.Range(
            [
              100
            ],
            [
              100
            ])
          .Count());
    }

    using (var db = new PlaneDB(di, planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual(
        100,
        db.Range(
            [
              0
            ],
            [
              99
            ])
          .Count());
      Assert.AreEqual(
        50,
        db.Range(
            [
              50
            ],
            [
              200
            ])
          .Count());

      Assert.AreEqual(
        50,
        db.Range(
            [
              1
            ],
            [
              50
            ])
          .Count());

      Assert.AreEqual(
        0,
        db.Range(
            [
              50
            ],
            [
              40
            ])
          .Count());

      Assert.AreEqual(
        0,
        db.Range(
            [
              200
            ],
            [
              210
            ])
          .Count());

      Assert.AreEqual(
        1,
        db.Range(
            [
              99
            ],
            [
              99
            ])
          .Count());
      Assert.AreEqual(
        1,
        db.Range(
            [
              0
            ],
            [
              0
            ])
          .Count());
      Assert.AreEqual(
        0,
        db.Range(
            [
              100
            ],
            [
              100
            ])
          .Count());
    }
  }

  [TestMethod]
  public void TestBasicRangeTyped()
  {
    var di = new DirectoryInfo(testDB);
    using (var db = new TypedPlaneDB<int, int>(
             new PlaneInt32Serializer(),
             new PlaneInt32Serializer(),
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 100; ++i) {
        db.SetValue(i, i);
      }

      Assert.AreEqual(100, db.Range(0, 99).Count());
      Assert.AreEqual(50, db.Range(50, 200).Count());
      Assert.AreEqual(50, db.Range(1, 50).Count());
      Assert.AreEqual(0, db.Range(50, 40).Count());
      Assert.AreEqual(0, db.Range(200, 210).Count());
      Assert.AreEqual(1, db.Range(99, 99).Count());
      Assert.AreEqual(1, db.Range(0, 0).Count());
      Assert.AreEqual(0, db.Range(100, 100).Count());
    }

    using (var db = new TypedPlaneDB<int, int>(
             new PlaneInt32Serializer(),
             new PlaneInt32Serializer(),
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      Assert.AreEqual(100, db.Range(0, 99).Count());
      Assert.AreEqual(50, db.Range(50, 200).Count());
      Assert.AreEqual(50, db.Range(1, 50).Count());
      Assert.AreEqual(0, db.Range(50, 40).Count());
      Assert.AreEqual(0, db.Range(200, 210).Count());
      Assert.AreEqual(1, db.Range(99, 99).Count());
      Assert.AreEqual(1, db.Range(0, 0).Count());
      Assert.AreEqual(0, db.Range(100, 100).Count());
    }
  }

  [TestMethod]
  public void TestCorrupt()
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = new PlaneOptions();
    using (var db = new StringPlaneDB(di, dbOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        var v = i.ToString() + i + i + i + i;
        if (i % 2 == 0) {
          db[k] = v;
        }
        else {
          db.SetValue(k, v);
        }
      }

      db.Flush();
    }

    // Corrupt some data
    {
      using var fs = new FileStream(
        Path.Combine(di.FullName, "default-0001.planedb"),
        FileMode.Open,
        FileAccess.ReadWrite,
        FileShare.None);
      _ = fs.Seek(17000, SeekOrigin.Begin);
      fs.WriteByte(0xfe);
    }

    _ = Assert.ThrowsExactly<IOException>(
      () => {
        using var db = new StringPlaneDB(di, dbOptions);
        for (var i = 0; i < COUNT; ++i) {
          var k = i.ToString();
          var v = i.ToString() + i + i + i + i;
          Assert.AreEqual(v, db[k]);
        }
      });

    var read = 0;
    var missing = 0;
    {
      var exceptions = new List<Exception>();
      using var db = new StringPlaneDB(
        di,
        dbOptions.ActivateRepairMode((_, args) => exceptions.Add(args.Reason)));
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        var v = i.ToString() + i + i + i + i;
        if (db.TryGetValue(k, out var ev)) {
          Assert.AreEqual(v, ev);
          read++;
        }
        else {
          missing++;
        }
      }

      Console.Error.WriteLine(
        $"exceptions: {exceptions.Count}, read: {read}, missing: {missing}");

      Assert.AreNotEqual(0, exceptions.Count);
      Assert.AreNotEqual(0, read);
      Assert.AreNotEqual(0, missing);
    }

    // Stays same after recovery
    {
      var readNow = 0;
      var missingNow = 0;
      using var db = new StringPlaneDB(di, dbOptions);
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        var v = i.ToString() + i + i + i + i;
        if (db.TryGetValue(k, out var ev)) {
          Assert.AreEqual(v, ev);
          readNow++;
        }
        else {
          missingNow++;
        }
      }

      Assert.AreEqual(read, readNow);
      Assert.AreEqual(missing, missingNow);
    }

    // Locked
    {
      using var fs = new FileStream(
        Path.Combine(di.FullName, "default-0001.planedb"),
        FileMode.Open,
        FileAccess.ReadWrite,
        FileShare.None);
      _ = Assert.ThrowsExactly<IOException>(
        () => {
          using var db = new StringPlaneDB(di, dbOptions);
          for (var i = 0; i < COUNT; ++i) {
            var k = i.ToString();
            var v = i.ToString() + i + i + i + i;
            Assert.AreEqual(v, db[k]);
          }
        });
    }

    // Stays same after locking error
    {
      var readNow = 0;
      var missingNow = 0;
      using var db = new StringPlaneDB(di, dbOptions);
      for (var i = 0; i < COUNT; ++i) {
        var k = i.ToString();
        var v = i.ToString() + i + i + i + i;
        if (db.TryGetValue(k, out var ev)) {
          Assert.AreEqual(v, ev);
          readNow++;
        }
        else {
          missingNow++;
        }
      }

      Assert.AreEqual(read, readNow);
      Assert.AreEqual(missing, missingNow);
    }
  }

  [TestMethod]
  public void TestJournalReplay()
  {
    var values = new SortedDictionary<string, string> {
      { "a", "1" },
      { "b", "2" },
      { "c", "3" },
      { "d", "4" }
    };
    var di = new DirectoryInfo(testDB);
    using (var db = new StringPlaneDB(
             di,
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      foreach (var (key, value) in values) {
        db[key] = value;
      }
    }

    byte[] salt;
    using (var db = new StringPlaneDB(di, planeOptions)) {
      salt = ((PlaneDB)db.BaseDB).Salt;
      db.Flush();
      Assert.IsTrue(
        db.OrderBy(i => i.Key)
          .SequenceEqual(
            values,
            new KVComparer<string, string>(
              StringComparer.Ordinal,
              StringComparer.Ordinal)));
    }

    using (var journalStream = new FileStream(
             Path.Combine(di.FullName, "default-JOURNAL.planedb"),
             FileMode.Create,
             FileAccess.ReadWrite,
             FileShare.None,
             40960,
             FileOptions.SequentialScan)) {
      using var journal = new Journal(
        journalStream,
        salt,
        planeOptions,
        new FakeReadWriteLock());
      journal.Put("b"u8, "5"u8);
      values["b"] = "5";
      journal.Put("e"u8, "6"u8);
      values["e"] = "6";
      journal.Remove("d"u8);
      _ = values.Remove("d");
      journal.Flush();
    }

    using (var db = new StringPlaneDB(di, planeOptions)) {
      db.Flush();
      var msg = string.Join(
        ", ",
        db.OrderBy(i => i.Key).Select(i => $"{i.Key}={i.Value}"));
      Assert.IsTrue(
        db.OrderBy(i => i.Key)
          .SequenceEqual(
            values,
            new KVComparer<string, string>(
              StringComparer.Ordinal,
              StringComparer.Ordinal)),
        msg);
    }

    _ = Assert.ThrowsExactly<PlaneDBBrokenJournalException>(
      () => {
        using (var journalStream = new FileStream(
                 Path.Combine(di.FullName, "default-JOURNAL.planedb"),
                 FileMode.Create,
                 FileAccess.ReadWrite,
                 FileShare.None,
                 40960,
                 FileOptions.SequentialScan)) {
          journalStream.Write("bogus"u8);
        }

        using var db = new StringPlaneDB(di, planeOptions);
        Assert.AreEqual(4, db.Count);
      });

    using (var journalStream = new FileStream(
             Path.Combine(di.FullName, "default-JOURNAL.planedb"),
             FileMode.Create,
             FileAccess.ReadWrite,
             FileShare.None,
             40960,
             FileOptions.SequentialScan)) {
      journalStream.Write("bogus"u8);
    }

    using (var db = new StringPlaneDB(di, planeOptions.SkipBrokenJournal())) {
      Assert.AreEqual(4, db.Count);
    }
  }

  [TestMethod]
  [DataRow(PlaneLevel0TargetSize.DefaultSize, CompactionMode.Normal)]
  [DataRow(PlaneLevel0TargetSize.DefaultSize, CompactionMode.Fully)]
  [DataRow(PlaneLevel0TargetSize.QuadrupleSize, CompactionMode.Normal)]
  public void TestLargeish(PlaneLevel0TargetSize level0TargetSize, CompactionMode mode)
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.WithLevel0TargetSize(level0TargetSize);

    var value = new byte[10240];
    value[0] = 1;
    value[4095] = 0xff;
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryGetValue(BitConverter.GetBytes(i), out var val));
        Assert.IsTrue(value.AsSpan().SequenceEqual(val));
      }
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      db.Compact(mode);
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryGetValue(BitConverter.GetBytes(i), out var val));
        Assert.IsTrue(value.AsSpan().SequenceEqual(val));
      }
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      for (var i = 0; i < 10000; ++i) {
        Assert.IsTrue(db.TryGetValue(BitConverter.GetBytes(i), out var val));
        Assert.IsTrue(value.AsSpan().SequenceEqual(val));
      }
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual(10000, db.Count);
      Assert.IsTrue(db.TryRemove(BitConverter.GetBytes(1000), out _));
      Assert.AreEqual(9999, db.Count);
      Assert.IsFalse(db.TryRemove(BitConverter.GetBytes(1000), out _));
      var read = db.Select((e, i) => new KeyValuePair<byte[], int>(e.Key, i)).Count();

      Assert.AreEqual(db.Count, read);
      db.RegisterMergeParticipant(new NullParticipant<byte[], byte[]>());
      db.RegisterMergeParticipant(new NullParticipant<byte[], byte[]>());
      db.Compact(mode);
      Assert.AreEqual(db.Count, read);
      db.RegisterMergeParticipant(new KillAllMergeParticipant<byte[], byte[]>());
      db.RegisterMergeParticipant(new NullParticipant<byte[], byte[]>());
      db.RegisterMergeParticipant(new NullParticipant<byte[], byte[]>());
      db.RegisterMergeParticipant(new NullParticipant<byte[], byte[]>());
      db.Compact(mode);
      Assert.AreEqual(0, db.Count);
    }

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.AreEqual(0, db.Count);
    }
  }

  [TestMethod]
  public void TestReadonly()
  {
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
        db[k] = v;
      }
    }

    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly)
               .WithKeyCacheMode(PlaneKeyCacheMode.ForceKeyCaching))) {
      using var db2 = new PlaneDB(
        new DirectoryInfo(testDB),
        planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly));
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        var v2 = Encoding.UTF8.GetString(db2[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
        Assert.AreEqual(v2, i.ToString() + i + i + i + i);
      }

      {
        // ReSharper disable AccessToDisposedClosure
        var k = new byte[10];
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.AddOrUpdate(k, k, (in byte[] _) => k));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.AddOrUpdate(k, () => k, (in byte[] _) => k));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.AddOrUpdate(k, _ => k, (in byte[] _, string _) => k, string.Empty));

        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.GetOrAdd(k, k));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.GetOrAdd(k, () => k));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.GetOrAdd(k, _ => k, string.Empty));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.GetOrAdd(k, k, out _));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryRemove(k, out _));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.TryRemove(
          [
            k
          ]));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.TryUpdate(k, k, k));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => db.TryUpdate(
            k,
            (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] value) => {
              value = k;

              return false;
            }));
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => _ = db.GetOrAddRange(
            [
              new KeyValuePair<byte[], byte[]>(k, k)
            ])
            .ToArray());
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => _ = db.GetOrAddRange(
              [
                k
              ],
              (in byte[] _) => k)
            .ToArray());
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => _ = db.GetOrAddRange(
              [
                k
              ],
              (in byte[] _, string _) => k,
              string.Empty)
            .ToArray());
        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(
          () => _ = db.GetOrAddRange(
              [
                k
              ],
              k)
            .ToArray());

        _ = Assert.ThrowsExactly<PlaneDBReadOnlyException>(() => db.Compact());
        // ReSharper restore AccessToDisposedClosure
      }
    }

    using (new PlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      _ = Assert.ThrowsExactly<PlaneDBAlreadyLockedException>(
        () => _ = new PlaneDB(
          new DirectoryInfo(testDB),
          planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly)));
    }

    using (new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      _ = Assert.ThrowsExactly<PlaneDBAlreadyLockedException>(
        () => _ = new PlaneDB(new DirectoryInfo(testDB), planeOptions));
    }
  }

  [TestMethod]
  public void TestRemoveOrphans()
  {
    var di = new DirectoryInfo(testDB);
    using (var db = new PlaneDB(
             di,
             planeOptions.DisableJournal().WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      var value = new byte[100];
      for (var i = 0; i < 10; ++i) {
        Assert.IsTrue(db.TryAdd(BitConverter.GetBytes(i), value));
      }
    }

    var junk = Path.Combine(di.FullName, "default-119191919191.planedb");
    File.WriteAllText(junk, "test");
    var junk2 = Path.Combine(di.FullName, "default-junk.planedb");
    File.WriteAllText(junk2, "test");

    // keep lock on file so that this orphan cannot be removed!
    using var fs = new FileStream(junk2, FileMode.Open, FileAccess.Read, FileShare.None);

    using (new PlaneDB(di, planeOptions)) {
      Assert.IsFalse(File.Exists(junk));
      Assert.AreEqual(OperatingSystem.IsWindows(), File.Exists(junk2));
    }
  }

  [TestMethod]
  public void TestSet()
  {
    var opts = planeOptions.WithEncryption("test1");
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             opts.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      db[[]] = [];
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
        db[k] = v;
      }
    }

    var packName = $"{testDB}/pack";
    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }

      db.WriteToPack(new FileInfo(packName), db.Options.WithEncryption("test2"));
      using var pack = new FileStream(
        packName,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None,
        1 * 1024 * 1024,
        FileOptions.SequentialScan);
      db.WriteToPack(pack, db.Options.WithEncryption("test3"));

      Assert.IsTrue(db.TryRemove("0"u8.ToArray(), out var removed));
      Assert.AreEqual("00000", Encoding.UTF8.GetString(removed));
    }

    using (var db = new PlaneDB(
             new DirectoryInfo(packName),
             opts.WithOpenMode(PlaneOpenMode.Packed).WithEncryption("test3"))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      for (var i = 1; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.IsTrue(db.ContainsKey(k));
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
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

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test1"u8.ToArray(), "test1"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test2"u8.ToArray(), "test2"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      _ = db.AddOrUpdate(
        "test2"u8.ToArray(),
        "test3"u8.ToArray(),
        (in byte[] _) => "test3"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test3"u8.ToArray(), "test4"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
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
    }
  }

  [TestMethod]
  [DataRow(2)]
  [DataRow(32)]
  [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
  [SuppressMessage(
    "CodeQuality",
    "IDE0079:Remove unnecessary suppression",
    Justification = "jb")]
  public void TestSetConcurrent(int concurrency)
  {
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.UsingTablespace(concurrency.ToString())
               .WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      using var barrier = new Barrier(concurrency);

      void Adder(int num)
      {
#pragma warning disable 8602
        barrier.SignalAndWait();
        for (var i = 0; i < COUNT; ++i) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          if (num == 0 && i % 250 == 0) {
            db.Flush();
          }

          switch ((i + num) % 10) {
            case 0:
              db[k] = v;

              break;
            case 1:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, v)));

              break;
            case 2:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, v, out _)));

              break;
            case 3:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, () => v)));

              break;
            case 4:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, _ => v, string.Empty)));

              break;
            case 5:
              Assert.IsTrue(v.SequenceEqual(db.AddOrUpdate(k, v, (in byte[] _) => v)));

              break;
            case 6:
              Assert.IsTrue(
                v.SequenceEqual(db.AddOrUpdate(k, () => v, (in byte[] _) => v)));

              break;
            case 7:
              Assert.IsTrue(
                v.SequenceEqual(
                  db.AddOrUpdate(k, _ => v, (in byte[] _, string _) => v, string.Empty)));

              break;
            case 8:
              _ = db.TryAdd(k, v);

              break;
            case 9:
              _ = db.TryAdd(k, v, out _);

              break;
          }
        }
#pragma warning restore 8602
      }

      var threads = Enumerable.Range(0, concurrency)
        .Select(num => new Thread(() => Adder(num)))
        .ToArray();
      foreach (var thread in threads) {
        thread.Start();
      }

      foreach (var thread in threads) {
        thread.Join();
      }
    }

    var j = 0;
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.UsingTablespace(concurrency.ToString()))) {
      void Reader()
      {
        int i;
        while ((i = Interlocked.Increment(ref j)) < COUNT) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
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
  [DataRow(32)]
  public void TestSetConcurrentSharedLock(int concurrency)
  {
    var options = planeOptions.WithDefaultLock();
    var j = 0;
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             options.UsingTablespace(concurrency.ToString())
               .WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      void Adder()
      {
        int i;
        while ((i = Interlocked.Increment(ref j)) < COUNT) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
          if (i % 250 == 0) {
            db.Flush();
          }

          switch (i % 4) {
            case 0:
              db[k] = v;

              break;
            case 1:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, v)));

              break;
            case 2:
              Assert.IsTrue(v.SequenceEqual(db.GetOrAdd(k, v, out _)));

              break;
            case 3:
              Assert.IsTrue(v.SequenceEqual(db.AddOrUpdate(k, v, (in byte[] _) => v)));

              break;
          }
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
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             options.UsingTablespace(concurrency.ToString()))) {
      void Reader()
      {
        int i;
        while ((i = Interlocked.Increment(ref j)) < COUNT) {
          var k = Encoding.UTF8.GetBytes(i.ToString());
          var v = Encoding.UTF8.GetString(db[k]);
          Assert.AreEqual(v, i.ToString() + i + i + i + i);
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
  public void TestSetFullySync()
  {
    var options = planeOptions.MakeFullySync();
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             options.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 10; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
        db[k] = v;
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), options)) {
      for (var i = 0; i < 10; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }
    }
  }

  [TestMethod]
  public void TestSetMixed()
  {
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 17; ++i) {
        var k = new byte[1 << i];
        db[k] = k;
        Assert.IsTrue(db.ContainsKey(k));

        k[0] = 1;
        var v = new byte[1 << (16 - i)];
        db[k] = v;
        Assert.IsTrue(db.ContainsKey(k));
      }

      for (var i = 0; i < 17; ++i) {
        var k = new byte[1 << i];
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(k));
        k[0] = 1;
        var v = new byte[1 << (16 - i)];
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(v));
      }

      foreach (var key in db.KeysIterator) {
        var v = new byte[key.Length];
        if (key[0] == 1) {
          v[0] = 1;
        }

        Assert.IsTrue(key.AsSpan().SequenceEqual(v));
      }

      Assert.AreEqual(34, db.KeysIterator.ToArray().Length);
      Assert.AreEqual(34, db.Keys.ToArray().Length);
      Assert.AreEqual(34, db.Values.ToArray().Length);
      Assert.AreEqual(34, db.Count);

      // Make sure we do not cause havoc if we throw
      db.OnFlushMemoryTable += (_, _) => throw new ArgumentException();
      db.OnMergedTables += (_, _) => throw new ArgumentException();

      db.Flush();

      for (var i = 0; i < 17; ++i) {
        var k = new byte[1 << i];
        Assert.IsTrue(db.ContainsKey(k));
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(k));
        k[0] = 1;
        var v = new byte[1 << (16 - i)];
        Assert.IsTrue(db.ContainsKey(k));
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(v));
      }

      foreach (var key in db.KeysIterator) {
        var v = new byte[key.Length];
        if (key[0] == 1) {
          v[0] = 1;
        }
      }

      Assert.AreEqual(34, db.KeysIterator.ToArray().Length);
      Assert.AreEqual(34, db.Keys.ToArray().Length);
      Assert.AreEqual(34, db.Values.ToArray().Length);
      Assert.AreEqual(34, db.Count);
      Assert.IsTrue(db.CurrentDiskSize > 0);
      Assert.IsTrue(db.CurrentBloomBits > 0);
      Assert.IsTrue(db.CurrentIndexBlockCount > 0);
    }

    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly))) {
      for (var i = 0; i < 17; ++i) {
        var k = new byte[1 << i];
        Assert.IsTrue(db.ContainsKey(k));
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(k));
        k[0] = 1;
        var v = new byte[1 << (16 - i)];
        Assert.IsTrue(db.ContainsKey(k));
        Assert.IsTrue(db[k].AsSpan().SequenceEqual(v));
      }

      foreach (var key in db.KeysIterator) {
        var v = new byte[key.Length];
        if (key[0] == 1) {
          v[0] = 1;
        }

        Assert.IsTrue(key.AsSpan().SequenceEqual(v));
      }

      Assert.AreEqual(34, db.KeysIterator.ToArray().Length);
      Assert.AreEqual(34, db.Keys.ToArray().Length);
      Assert.AreEqual(34, db.Values.ToArray().Length);
      Assert.AreEqual(34, db.Count);
    }
  }

  [TestMethod]
  public void TestSetMostlySync()
  {
    var options = planeOptions.MakeMostlySync();
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             options.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < 100; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
        db[k] = v;
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), options)) {
      for (var i = 0; i < 100; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }
    }
  }

  [TestMethod]
  public void TestSetThreadUnsafe()
  {
    var opts = planeOptions.DisableThreadSafety();
    using (var db = new PlaneDB(
             new DirectoryInfo(testDB),
             opts.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
        db[k] = v;
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      for (var i = 0; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }

      Assert.IsTrue(db.TryRemove("0"u8.ToArray(), out var removed));
      Assert.AreEqual("00000", Encoding.UTF8.GetString(removed));
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      for (var i = 1; i < COUNT; ++i) {
        var k = Encoding.UTF8.GetBytes(i.ToString());
        var v = Encoding.UTF8.GetString(db[k]);
        Assert.IsTrue(db.ContainsKey(k));
        Assert.AreEqual(v, i.ToString() + i + i + i + i);
      }
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
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

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test1"u8.ToArray(), "test1"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test2"u8.ToArray(), "test2"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      _ = db.AddOrUpdate(
        "test2"u8.ToArray(),
        "test3"u8.ToArray(),
        (in byte[] _) => "test3"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
      db.Add("test3"u8.ToArray(), "test4"u8.ToArray());
    }

    using (var db = new PlaneDB(new DirectoryInfo(testDB), opts)) {
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
    }
  }

  [TestMethod]
  public void TestTryUpdate()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
      Assert.IsFalse(db.TryUpdate(k, v, []));
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(db.TryUpdate(k, v, []));
      Assert.IsTrue(db.TryUpdate(k, [], v));
      Assert.AreEqual(0, db[k].Length);
    }
  }

  [TestMethod]
  public void TestTryUpdateFactory()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    for (var i = 0; i < COUNT; ++i) {
      var k = Encoding.UTF8.GetBytes(i.ToString());
      var v = Encoding.UTF8.GetBytes(i.ToString() + i + i + i + i);
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] _) =>
            throw new ArgumentException()));
      Assert.IsTrue(db.TryAdd(k, v));
      Assert.IsFalse(
        db.TryUpdate(
          k,
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return false;
          }));
      Assert.IsTrue(
        db.TryUpdate(
          k,
          (in byte[] _, in byte[] _, [MaybeNullWhen(false)] out byte[] o) => {
            o = [];

            return true;
          }));
      Assert.AreEqual(0, db[k].Length);
    }
  }

  [TestMethod]
  public void TestTryUpdateFactoryArg()
  {
    using var db = new PlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    TestTryUpdateFactoryArgTypedHelper(
      db,
      [],
      PlaneByteArrayComparer.Default,
      i => (Encoding.UTF8.GetBytes(i.ToString()),
        Encoding.UTF8.GetBytes(i.ToString() + i + i + i)));
  }

  [TestMethod]
  public void TestTryUpdateFactoryArgTyped()
  {
    using var db = new StringPlaneDB(
      new DirectoryInfo(testDB),
      planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite));
    TestTryUpdateFactoryArgTypedHelper(
      db,
      string.Empty,
      StringComparer.Ordinal,
      i => (i.ToString(), i.ToString() + i + i + i));
  }

  [TestMethod]
  public void TestTyped()
  {
    using (var db = new StringPlaneDB(
             new DirectoryInfo(testDB),
             planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      for (var i = 0; i < COUNT; ++i) {
        db[i.ToString()] = i.ToString() + i + i + i + i;
      }
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 0; i < COUNT; ++i) {
        Assert.AreEqual(db[i.ToString()], i.ToString() + i + i + i + i);
      }

      Assert.IsTrue(db.TryRemove("0", out var removed));
      Assert.AreEqual("00000", removed);
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      for (var i = 1; i < COUNT; ++i) {
        Assert.AreEqual(db[i.ToString()], i.ToString() + i + i + i + i);
      }
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
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

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      db.Add("test1", "test1");
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      db.Add("test2", "test2");
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      _ = db.AddOrUpdate("test2", "test3", (in string _) => "test3");
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      db.Add("test3", "test4");
      var arr = new KeyValuePair<string, string>[db.Count + 100];
      db.CopyTo(arr, 0);
      db.CopyTo(arr, 100);
      Assert.AreEqual(arr[0].Value, arr[100].Value);
      Assert.AreEqual(arr[0].Key, arr[100].Key);
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      foreach (var i in new[] { [
                   "test1",
                   "test1"
                 ], [
                   "test2",
                   "test3"
                 ],
                 new[] { "test3", "test4" }
               }) {
        Assert.IsTrue(db.TryGetValue(i[0], out var v));
        Assert.AreEqual(i[1], v);
      }
    }

    using (var db = new StringPlaneDB(new DirectoryInfo(testDB), planeOptions)) {
      var c = db.Count;
      Assert.AreNotEqual(0, c);
      var nullParticipant = new NullParticipant<string, string>();
      db.RegisterMergeParticipant(nullParticipant);
      db.RegisterMergeParticipant(new NullParticipant<string, string>());
      db.Compact();
      Assert.AreEqual(c, db.Count);
      db.RegisterMergeParticipant(new KillAllMergeParticipant<string, string>());
      db.RegisterMergeParticipant(new NullParticipant<string, string>());
      db.RegisterMergeParticipant(new NullParticipant<string, string>());
      db.Compact();
      Assert.AreEqual(0, db.Count);

      db.UnregisterMergeParticipant(nullParticipant);
    }
  }

  [TestMethod]
  [DataRow(PlaneKeyCacheMode.AutoKeyCaching)]
  [DataRow(PlaneKeyCacheMode.ForceKeyCaching)]
  [DataRow(PlaneKeyCacheMode.NoKeyCaching)]
  public void TestValueSizes(PlaneKeyCacheMode keyCacheMode)
  {
    var di = new DirectoryInfo(testDB);
    var dbOptions = planeOptions.WithKeyCacheMode(keyCacheMode);
    using (var db = new PlaneDB(
             di,
             dbOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite))) {
      db.SetValue([], []);
      for (var i = 0; i < 25; ++i) {
        var k = new[] { (byte)i };
        var v = new byte[1 << i];
        Trace.WriteLine(v.Length);
        v[0] = 1;
        v[^1] = 2;
        db.SetValue(k, v);
        Assert.IsTrue(db.TryGetValue(k, out var e));
        Assert.IsTrue(v.AsSpan().SequenceEqual(e));

        if (i == 11) {
          db.SetValue(v, v);
          Assert.IsTrue(db.ContainsKey(v));
          Assert.IsTrue(db.TryGetValue(v, out e));
          Assert.IsTrue(v.AsSpan().SequenceEqual(e));
        }

        k = [
          (byte)i,
          (byte)i
        ];
        db.SetValue(k, v);
        k = [
          (byte)i,
          (byte)i,
          (byte)i
        ];
        db.SetValue(k, v);
        if (i != 22) {
          continue;
        }

        for (var j = 0; j < 20; ++j) {
          k = [.. Enumerable.Repeat((byte)i, j + 4)];
          db.SetValue(k, v);
        }
      }
    }

    var cmp = PlaneByteArrayComparer.Default;

    using (var db = new PlaneDB(di, dbOptions)) {
      Assert.IsTrue(db.ContainsKey([]));
      Assert.IsTrue(db.KeysIterator.Contains([], cmp));
      Assert.IsTrue(db.TryGetValue([], out var e));
      Assert.AreEqual(0, e.Length);

      for (var i = 0; i < 25; ++i) {
        var k = new[] { (byte)i };
        var v = new byte[1 << i];
        v[0] = 1;
        v[^1] = 2;
        Assert.IsTrue(db.ContainsKey(k));
        Assert.IsTrue(db.KeysIterator.Contains(k, cmp));
        Assert.IsTrue(db.TryGetValue(k, out e));
        Assert.IsTrue(v.AsSpan().SequenceEqual(e));

        if (i != 11) {
          continue;
        }

        Assert.IsTrue(db.ContainsKey(v));
        Assert.IsTrue(db.TryGetValue(v, out e));
        Assert.IsTrue(v.AsSpan().SequenceEqual(e));
      }
    }
  }

  private sealed class
    KillAllMergeParticipant<TKey, TValue> : IPlaneDBMergeParticipant<TKey, TValue>
  {
    public bool Equals(IPlaneDBMergeParticipant<TKey, TValue>? other)
    {
      return ReferenceEquals(this, other);
    }

    public bool IsDataStale(in TKey key, in TValue value)
    {
      return true;
    }
  }

  private sealed class XORSaltableTransformer : IPlaneSaltableBlockTransformer
  {
    private readonly byte[] salted;

    internal XORSaltableTransformer(ReadOnlySpan<byte> salt)
    {
      salted = salt.ToArray();
    }

    public bool MayChangeSize => false;

    public ReadOnlySpan<byte> TransformBlock(ReadOnlySpan<byte> block)
    {
      var rv = block.ToArray();
      var m = Math.Min(salted.Length, rv.Length);
      for (var i = 0; i < m; i++) {
        rv[i] ^= salted[i];
      }

      return rv;
    }

    public int UntransformBlock(ReadOnlySpan<byte> input, Span<byte> block)
    {
      input.CopyTo(block);
      var m = Math.Min(salted.Length, input.Length);
      for (var i = 0; i < m; i++) {
        block[i] ^= salted[i];
      }

      return input.Length;
    }

    public IBlockTransformer GetTransformerFor(ReadOnlySpan<byte> salt)
    {
      return new XORSaltableTransformer(salt);
    }
  }
}

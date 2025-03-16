using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using NMaier.PlaneDB.RedisProtocol;

using StackExchange.Redis;

namespace NMaier.PlaneDB.Tests;

[TestClass]
public sealed class RemoteRedisTests
{
  private static readonly PlaneOptions planeOptions =
    new PlaneOptions(PlaneOpenMode.ExistingReadWrite).WithCompression();

  private static readonly string testdb = $"{Path.GetTempPath()}/PlaneTestDB";

  private static bool Match(string a, string b)
  {
    return RedisExtensions.StringMatch(
      Encoding.UTF8.GetBytes(a),
      Encoding.UTF8.GetBytes(b));
  }

  private static void Run(
    [InstantHandle] Action<IPlaneDBRemote, IDatabase, IPlaneDB<byte[], byte[]>> runner)
  {
    using var token = new CancellationTokenSource();
    using var db = new PlaneDB(new DirectoryInfo(testdb), planeOptions);

    using var remote = db.ServeRedis(
      new PlaneDBRemoteOptions("test") { Address = IPAddress.Loopback },
      token.Token);
    var connString =
      $"localhost:{remote.Port},password=test,connectTimeout=100,connectRetry=10";
    Debug.WriteLine(connString);
    using var client = ConnectionMultiplexer.Connect(connString);

    var rdb = client.GetDatabase();
    Assert.AreEqual("OK", rdb.Execute("FLUSHALL").ToString());

    runner(remote, rdb, db);
  }

  private static async Task TestReadNextInternal(RespParser parser)
  {
    var r = await parser.ReadNext(CancellationToken.None);
    Assert.IsFalse(r.IsNull);
    var arr = r.AsArray();
    Assert.IsFalse(arr.Any(e => e.IsNull));
    Assert.AreEqual(2, arr.Length);
    var a1 = arr[0].AsArray();
    Assert.AreEqual(1, a1[0].AsLong());
    Assert.AreEqual(2, a1[1].AsLong());
    Assert.AreEqual(3, a1[2].AsLong());
    var a2 = arr[1].AsArray();
    Assert.AreEqual(4, a2.Length);
    Assert.AreEqual("Foo", a2[0].AsString());
    Assert.AreEqual("Bar", a2[1].AsString());
    Assert.IsTrue(a2[1].AsBytes().AsSpan().SequenceEqual("Bar"u8));
    Assert.IsTrue(a2[2].IsNull);
    Assert.AreEqual(null, a2[2].AsNullableString());
    Assert.IsTrue(a2[3].IsNull);

    Assert.AreEqual("OK", (await parser.ReadNext(CancellationToken.None)).AsString());
    var ex = await Assert.ThrowsExceptionAsync<RespResponseException>(
      () => parser.ReadNext(CancellationToken.None));
    Assert.AreEqual("FAIL", ex.Message);

    var a3 = (await parser.ReadNext(CancellationToken.None)).AsArray();
    Assert.AreEqual(0, a3.Length);

    var ex2 =
      await Assert.ThrowsExceptionAsync<RespProtocolException>(
        () => parser.ReadNext(CancellationToken.None));
    Assert.IsTrue(ex2.Message.Contains("Truncated"));
  }

  [TestMethod]
  public void TestMatcher()
  {
    Assert.IsTrue(Match("test2", "test2"), "full");
    Assert.IsTrue(Match("test2", "test*"), "suffix *");
    Assert.IsTrue(Match("test22", "test*"), "suffix *");
    Assert.IsTrue(Match("test2", "test?"), "suffix ?");
    Assert.IsFalse(Match("test22", "test?"), "suffix ?");
    Assert.IsTrue(Match("test2", "test?"), "suffix ?");
    Assert.IsFalse(Match("test22", "test?"), "suffix ?");

    Assert.IsTrue(Match("test2", "*test2"), "prefix *");
    Assert.IsTrue(Match("test2", "*est2"), "prefix *");
    Assert.IsTrue(Match("test2", "?est2"), "prefix ?");
    Assert.IsFalse(Match("test2", "?st2"), "prefix ?");

    Assert.IsTrue(Match("test2", "t?st2"), "prefix ?");
    Assert.IsTrue(Match("test2", "te?t2"), "prefix ?");
    Assert.IsFalse(Match("test2", "t?t2"), "prefix ?");

    Assert.IsTrue(Match("test2", "*test*"), "* enclosed *");
    Assert.IsTrue(Match("test2", "*t*st*"), "* enclosed and middle *");

    Assert.IsTrue(Match("test2", "[te]est2"), "first group");
    Assert.IsTrue(Match("test2", "[te]est*"), "first group + *");

    Assert.IsTrue(Match("test2", "t[te]st2"), "2nd group");
    Assert.IsTrue(Match("test2", "t[te]st*"), "2nd group + *");
    Assert.IsFalse(Match("test2", "t[^te]st2"), "2nd group");
    Assert.IsFalse(Match("test2", "t[^te]st*"), "2nd group + *");

    Assert.IsTrue(Match("test2", "t[e-t]st2"), "2nd group span");
    Assert.IsTrue(Match("test2", "t[^a-d]st2"), "2nd group span");
    Assert.IsFalse(Match("test2", "t[^e-t]st2"), "2nd group span");
    Assert.IsFalse(Match("test2", "t[a-d]st2"), "2nd group span");
  }

  [TestMethod]
  public async Task TestParserReadNext()
  {
    await using var ms =
      "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*4\r\n+Foo\r\n$3\r\nBar\r\n$-1\r\n*-1\r\n+OK\r\n-FAIL\r\n*0\r\n"
        .AsMemoryStream();
    var parser = new RespParser(ms);
    await TestReadNextInternal(parser);
  }

  [TestMethod]
  public async Task TestParserReadNextLongOverflow()
  {
    var parser = new RespParser(
      ":11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
        .AsMemoryStream());

    _ = await Assert.ThrowsExceptionAsync<RespProtocolException>(
      () => parser.ReadNext(CancellationToken.None));
  }

  [TestMethod]
  public async Task TestParserWrite()
  {
    await using var ms = new MemoryStream();
    var parser = new RespParser(ms);

    var wa1 = new RespArray(new RespInteger(1), new RespInteger(2), new RespInteger(3));
    var wa2 = new RespArray(
      new RespString("Foo"),
      new RespBulkString("Bar"),
      RespNullString.Value,
      RespNullArray.Value);

    await parser.Write(new RespArray(wa1, wa2), CancellationToken.None);
    await parser.Write("OK", CancellationToken.None);
    await parser.Write(new RespErrorString("FAIL"), CancellationToken.None);
    await parser.Write(new RespArray(), CancellationToken.None);

    _ = ms.Seek(0, SeekOrigin.Begin);

    await TestReadNextInternal(parser);
  }

  [TestMethod]
  [SuppressMessage(
    "Performance",
    "CA1861:Avoid constant arrays as arguments",
    Justification = "<Pending>")]
  public void TestRedis()
  {
    Run(
      (remote, rdb, db) => {
        _ = Assert.ThrowsException<RedisConnectionException>(
          () => {
            using var badauth = ConnectionMultiplexer.Connect(
              $"localhost:{remote.Port},,connectTimeout=100,connectRetry=0");
          });

        _ = Assert.ThrowsException<RedisConnectionException>(
          () => {
            using var badauth = ConnectionMultiplexer.Connect(
              $"localhost:{remote.Port},password=test2,connectTimeout=100,connectRetry=0");
          });

        // Basic set
        Assert.IsTrue(rdb.StringSet("test", "success"));
        Assert.AreEqual("success", rdb.StringGet("test").ToString());

        // Append
        Assert.AreEqual(
          "successandsuccess".Length,
          rdb.StringAppend("test", "andsuccess"));
        Assert.AreEqual("successandsuccess", rdb.StringGet("test").ToString());
        Assert.AreEqual("andsuccess".Length, rdb.StringAppend("test2", "andsuccess"));
        Assert.AreEqual("andsuccess", rdb.StringGet("test2").ToString());

        // GetSet
        Assert.AreEqual(
          "successandsuccess",
          rdb.StringGetSet("test", "alsosucccess").ToString());
        Assert.AreEqual(
          "alsosucccess",
          rdb.StringGetSet("test", "successandsuccess").ToString());
        Assert.AreEqual(null!, rdb.StringGetSet("test3", "alsosucccess").ToString());
        Assert.AreEqual(
          "alsosucccess",
          rdb.StringGetSet("test3", "successandsuccess").ToString());

        Assert.IsTrue(rdb.KeyDelete("test3"));
        Assert.IsFalse(rdb.KeyDelete("test3"));

        // MSET/MGET

        Assert.IsTrue(rdb.StringSet(Seq(0).ToArray()));
        Assert.IsFalse(rdb.StringSet(Seq(2).ToArray(), When.NotExists));

        var seq = rdb.StringGet(Seq(0).Select(i => i.Key).ToArray());
        Assert.AreEqual(10, seq.Length);
        Assert.IsTrue(!seq[9].IsNull);
        seq = rdb.StringGet(Seq(2).Select(i => i.Key).ToArray());
        Assert.AreEqual(10, seq.Length);
        Assert.IsTrue(seq[9].IsNull);

        // Del
        Assert.AreEqual(8, rdb.KeyDelete(Seq(2).Select(i => i.Key).ToArray()));
        Assert.AreEqual(0, rdb.KeyDelete(Seq(2).Select(i => i.Key).ToArray()));

        // Basic ttl
        Assert.AreEqual(null, rdb.KeyTimeToLive("test"));
        Assert.IsTrue(rdb.KeyExists("test"));
        Assert.AreEqual(
          1,
          rdb.KeyExists(
          [
            (RedisKey)"test"
          ]));
        Assert.IsTrue(rdb.KeyExpire("test", TimeSpan.FromSeconds(50)));
        Assert.AreNotEqual(null, rdb.KeyTimeToLive("test"));
        Assert.IsTrue(
          rdb.KeyExpire("test", DateTime.UtcNow + TimeSpan.FromMilliseconds(10)));
        Thread.Sleep(50);
        Assert.IsFalse(
          rdb.KeyExpire("test", DateTime.UtcNow + TimeSpan.FromMilliseconds(50)));
        Assert.IsFalse(rdb.KeyExists("test"));
        Assert.AreEqual(
          0,
          rdb.KeyExists(
          [
            (RedisKey)"test"
          ]));

        // Basic ttl
        Assert.IsTrue(rdb.StringSet("test", "success"));
        Assert.AreEqual(null, rdb.KeyTimeToLive("test"));
        Assert.IsTrue(rdb.KeyExists("test"));
        Assert.AreEqual(
          1,
          rdb.KeyExists(
          [
            (RedisKey)"test"
          ]));
        Assert.IsTrue(rdb.StringSet("test", "success", TimeSpan.FromSeconds(50)));
        Assert.AreNotEqual(null, rdb.KeyTimeToLive("test"));
        Assert.IsTrue(rdb.StringSet("test", "success", TimeSpan.FromMilliseconds(10)));
        Thread.Sleep(50);
        Assert.IsFalse(rdb.KeyExists("test"));
        Assert.AreEqual(
          0,
          rdb.KeyExists(
          [
            (RedisKey)"test"
          ]));

        Assert.AreEqual(null, rdb.KeyTimeToLive("test2"));
        Assert.IsTrue(rdb.KeyExpire("test2", TimeSpan.FromSeconds(50)));
        Assert.AreNotEqual(null, rdb.KeyTimeToLive("test2"));
        Assert.IsTrue(rdb.KeyPersist("test2"));
        Assert.AreEqual(null, rdb.KeyTimeToLive("test2"));

        Assert.IsTrue(rdb.KeyRename("test2", "test", When.NotExists));
        Assert.IsFalse(rdb.KeyRename("test2", "test", When.NotExists));
        Assert.IsTrue(rdb.KeyRename("test", "test2"));
        Assert.AreEqual("andsuccess", rdb.StringGet("test2").ToString());
        Assert.IsTrue(rdb.KeyRename("test", "test2"));
        Assert.AreEqual(null!, rdb.StringGet("test2").ToString());

        // Advanced Set
        Assert.IsFalse(rdb.StringSet("xx", "xx", when: When.Exists));
        Assert.IsFalse(
          rdb.StringSet("xx", "xx", TimeSpan.FromSeconds(10), when: When.Exists));
        Assert.IsTrue(rdb.StringSet("test2", "nx"));
        Assert.IsFalse(rdb.StringSet("test2", "nx", when: When.NotExists));
        Assert.IsFalse(
          rdb.StringSet("test2", "nx", TimeSpan.FromSeconds(10), when: When.NotExists));
        Assert.IsTrue(rdb.StringSet("test2", "xx", when: When.Exists));
        Assert.IsTrue(
          rdb.StringSet("test2", "xx", TimeSpan.FromSeconds(10), when: When.Exists));
        Assert.IsFalse(
          rdb.StringSet("test2", "xx", TimeSpan.FromSeconds(10), when: When.NotExists));

        // Key tests
        Assert.AreEqual(db.Count.ToString(), rdb.Execute("DBSIZE").ToString());
        Assert.AreEqual(3, db.Count);
        var dict = new Dictionary<byte[], byte[]>(PlaneByteArrayComparer.Default);
        db.CopyTo(dict);
        using var db2 = new PlaneDB(
          db.Location,
          db.Options.WithOpenMode(PlaneOpenMode.ReadWrite).UsingTablespace("2"));
        db.CopyTo(db2);

        Assert.IsTrue(
          new[] { "a0", "a1", "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "*")),
          "all keys");
        Assert.IsTrue(
          new[] { "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "test*")),
          "suffix *");
        Assert.IsTrue(
          new[] { "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "*test2")),
          "prefix *");
        Assert.IsTrue(
          new[] { "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "*test*")),
          "enclosed *");
        Assert.IsTrue(
          new[] { "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "t*est*")),
          "middle *");
        Assert.IsTrue(
          new[] { "test2" }.SequenceEqual((string[])rdb.Execute("KEYS", "t?st*")),
          "wild ?");
        Assert.IsTrue(
          new[] { "a0", "a1" }.SequenceEqual((string[])rdb.Execute("KEYS", "a?")),
          "end ?");

        // IncrDecr
        Assert.AreEqual(1, rdb.StringIncrement("iv"));
        Assert.AreEqual(2, rdb.StringIncrement("iv"));
        Assert.AreEqual(4, rdb.StringIncrement("iv", 2));

        Assert.AreEqual(-1, rdb.StringDecrement("dv"));
        Assert.AreEqual(-2, rdb.StringDecrement("dv"));
        Assert.AreEqual(-4, rdb.StringDecrement("dv", 2));

        _ = Assert.ThrowsException<RedisServerException>(
          () => rdb.StringIncrement("test2"));

        // Some flushing
        Assert.AreEqual("OK", rdb.Execute("FLUSHALL").ToString());
        Assert.AreEqual("OK", rdb.Execute("FLUSHDB").ToString());
        Assert.AreEqual("0", rdb.Execute("DBSIZE").ToString());

        return;

        static IEnumerable<KeyValuePair<RedisKey, RedisValue>> Seq(int off)
        {
          for (var i = 0; i < 10; ++i) {
            yield return new KeyValuePair<RedisKey, RedisValue>($"a{off + i}", $"b{i}");
          }
        }
      });
  }

  [TestMethod]
  [SuppressMessage(
    "Performance",
    "CA1861:Avoid constant arrays as arguments",
    Justification = "<Pending>")]
  public void TestRedisBits()
  {
    Run(
      (_, rdb, _) => {
        Assert.IsFalse(rdb.StringSetBit("bit", 1, true));
        var s = (byte[])rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0b01000000, s[0]);

        Assert.IsTrue(rdb.StringSetBit("bit", 1, false));
        s = rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0, s[0]);

#pragma warning disable IDE0058 // Expression value is never used
        rdb.KeyDelete("bit");
        rdb.StringSetBit("bit", 2, true);
        rdb.StringSetBit("bit", 3, true);
        rdb.StringSetBit("bit", 5, true);
        rdb.StringSetBit("bit", 10, true);
        rdb.StringSetBit("bit", 11, true);
        rdb.StringSetBit("bit", 14, true);
        string fortytwo = rdb.StringGet("bit");
        Assert.AreEqual("42", fortytwo);

        rdb.StringSet("bit", "@");
        Assert.IsFalse(rdb.StringSetBit("bit", 2, true));
        s = rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0b01100000, s[0]);

        Assert.IsTrue(rdb.StringSetBit("bit", 1, false));
        s = rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0b00100000, s[0]);

        rdb.KeyDelete("bit");
        rdb.StringIncrement("bit");
        Assert.IsFalse(rdb.StringSetBit("bit", 6, true));
        s = rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0b00110011, s[0]);
        Assert.IsTrue(rdb.StringSetBit("bit", 2, false));
        s = rdb.StringGet("bit");
        Assert.AreEqual(1, s.Length);
        Assert.AreEqual(0b00010011, s[0]);

        Assert.ThrowsException<RedisServerException>(
          () => rdb.StringSetBit("bit", 4294967296L, true));
        Assert.ThrowsException<RedisServerException>(
          () => rdb.StringSetBit("bit", -1, true));

        rdb.KeyDelete("bit");
        Assert.IsFalse(rdb.StringGetBit("bit", 1));

        Assert.IsTrue(rdb.StringSet("bit", "`"));
        Assert.IsFalse(rdb.StringGetBit("bit", 0));
        Assert.IsTrue(rdb.StringGetBit("bit", 1));
        Assert.IsTrue(rdb.StringGetBit("bit", 2));
        Assert.IsFalse(rdb.StringGetBit("bit", 3));

        Assert.IsFalse(rdb.StringGetBit("bit", 8));
        Assert.IsFalse(rdb.StringGetBit("bit", 100));
        Assert.IsFalse(rdb.StringGetBit("bit", 1000));

        rdb.KeyDelete("bit");
        rdb.StringIncrement("bit");
        Assert.IsFalse(rdb.StringGetBit("bit", 0));
        Assert.IsFalse(rdb.StringGetBit("bit", 1));
        Assert.IsTrue(rdb.StringGetBit("bit", 2));
        Assert.IsTrue(rdb.StringGetBit("bit", 3));

        Assert.IsFalse(rdb.StringGetBit("bit", 8));
        Assert.IsFalse(rdb.StringGetBit("bit", 100));
        Assert.IsFalse(rdb.StringGetBit("bit", 1000));

        var vec = new[] { "", "\xaa", "\0\0\xff", "foobar", "123" };
        foreach (var v in vec) {
          var bytes = Encoding.ASCII.GetBytes(v);
          rdb.StringSet("bit", bytes);
          Assert.IsTrue(bytes.SequenceEqual((byte[])rdb.StringGet("bit")));
          Assert.AreEqual(
            BitCountSlow(bytes),
            rdb.StringBitCount("bit"),
            $"bitcount {v})");
        }

        var foobar = "foobar"u8.ToArray();
        rdb.StringSet("bit", foobar);
        Assert.AreEqual(BitCountSlow(foobar), rdb.StringBitCount("bit"));
        Assert.AreEqual(
          BitCountSlow(foobar.AsSpan(1, foobar.Length - 2).ToArray()),
          rdb.StringBitCount("bit", 1, -2));
        Assert.AreEqual(0, rdb.StringBitCount("bit", -2, 1));
        Assert.AreEqual(BitCountSlow(foobar), rdb.StringBitCount("bit", 0, 1000));

        rdb.StringSet("bit", "ab"u8.ToArray());
        Assert.AreEqual(3, rdb.StringBitCount("bit", 1));

        rdb.StringSet("bit", "__PPxxxxxxxxxxxxxxxxRR__"u8.ToArray());
        Assert.AreEqual(74, rdb.StringBitCount("bit", 2, -3));

#pragma warning restore IDE0058 // Expression value is never used
      });

    return;

    static long BitCountSlow(byte[] b)
    {
      var ba = new BitArray(b);

      return (from bool m in ba where m select m).LongCount();
    }
  }

  [TestMethod]
  [SuppressMessage(
    "Performance",
    "CA1861:Avoid constant arrays as arguments",
    Justification = "<Pending>")]
  public void TestRedisList()
  {
    Run(
      (__, rdb, db) => {
        _ = rdb.KeyDelete("mylist");
        Assert.AreEqual(
          8,
          rdb.ListRightPush(
            "mylist",
            [
              "a",
              "b",
              "c",
              "1",
              "2",
              "3",
              "c",
              "c"
            ]));
        Assert.AreEqual(0, (long)rdb.Execute("LPOS", "mylist", "a"));
        Assert.AreEqual(2, (long)rdb.Execute("LPOS", "mylist", "c"));
        Assert.IsTrue(
          new[] { 2L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", "1")));
        Assert.IsTrue(
          new[] { 2L, 6L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", "2")));
        Assert.AreEqual(6, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", "2"));
        Assert.AreEqual(6, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", "-2"));
        Assert.AreEqual(7, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", "-1"));
        Assert.IsTrue(
          new[] { 7L, 6L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "RANK", "-1", "COUNT", "2")));

        Assert.AreEqual(2, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", 1));
        Assert.AreEqual(6, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", 2));
        Assert.AreEqual(null, (string?)rdb.Execute("LPOS", "mylist", "c", "RANK", 4));
        Assert.AreEqual(7, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", -1));
        Assert.AreEqual(6, (long)rdb.Execute("LPOS", "mylist", "c", "RANK", -2));

        Assert.IsTrue(
          new[] { 2L, 6L, 7L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 0)));
        Assert.IsTrue(
          new[] { 2L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 1)));
        Assert.IsTrue(
          new[] { 2L, 6L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 2)));
        Assert.IsTrue(
          new[] { 2L, 6L, 7L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 100)));

        Assert.IsTrue(
          new[] { 6L, 7L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 0, "RANK", 2)));
        Assert.IsTrue(
          new[] { 7L, 6L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 2, "RANK", -1)));

        Assert.IsTrue(
          Array.Empty<long>()
            .SequenceEqual(
              (long[])rdb.Execute("LPOS", "mylist xxx", "c", "COUNT", 0, "RANK", 2)));

        Assert.IsTrue(
          Array.Empty<long>()
            .SequenceEqual(
              (long[])rdb.Execute("LPOS", "mylist", "x", "COUNT", 2, "RANK", -1)));
        Assert.AreEqual(null, (string?)rdb.Execute("LPOS", "mylist", "x", "RANK", -1));

        Assert.IsTrue(
          new[] { 0L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "a", "COUNT", 0, "MAXLEN", 1)));
        Assert.IsTrue(
          Array.Empty<long>()
            .SequenceEqual(
              (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 0, "MAXLEN", 1)));
        Assert.IsTrue(
          new[] { 2L }.SequenceEqual(
            (long[])rdb.Execute("LPOS", "mylist", "c", "COUNT", 0, "MAXLEN", 3)));
        Assert.IsTrue(
          new[] { 7L, 6L }.SequenceEqual(
            (long[])rdb.Execute(
              "LPOS",
              "mylist",
              "c",
              "COUNT",
              0,
              "MAXLEN",
              3,
              "RANK",
              -1)));
        Assert.IsTrue(
          new[] { 6L }.SequenceEqual(
            (long[])rdb.Execute(
              "LPOS",
              "mylist",
              "c",
              "COUNT",
              0,
              "MAXLEN",
              7,
              "RANK",
              2)));

        _ = rdb.KeyDelete("mylist");
        _ = rdb.ListLeftPush("mylist", "a");
        Assert.IsTrue(
          Array.Empty<long>()
            .SequenceEqual(
              (long[])rdb.Execute("LPOS", "mylist", "b", "COUNT", 10, "RANK", 5)));

        Assert.AreEqual(null, (string?)rdb.Execute("LPOP", "non-existent-list"));

        Assert.AreEqual(
          7,
          rdb.ListLeftPush(
            "listcount",
            [
              "aa",
              "bb",
              "cc",
              "dd",
              "ee",
              "ff",
              "gg"
            ]));
        Assert.AreEqual(7, rdb.ListLength("listcount"));
        rdb.ListSetByIndex("listcount", 1, "fff");
        rdb.ListSetByIndex("listcount", 0, "ggg");
        Assert.AreEqual("ggg", (string)rdb.ListGetByIndex("listcount", 0));
        Assert.AreEqual("fff", (string)rdb.ListGetByIndex("listcount", 1));
        Assert.AreEqual("aa", (string)rdb.ListGetByIndex("listcount", 6));
        Assert.AreEqual("bb", (string)rdb.ListGetByIndex("listcount", 5));
        Assert.AreEqual(null, (string?)rdb.ListGetByIndex("listcount", 7));
        Assert.AreEqual("aa", (string)rdb.ListGetByIndex("listcount", -1));
        Assert.AreEqual("bb", (string)rdb.ListGetByIndex("listcount", -2));

        Assert.IsTrue(
          Array.Empty<RedisValue>()
            .SequenceEqual((RedisValue[])rdb.Execute("LPOP", "listcount", 0)));

        rdb.ListSetByIndex("listcount", -2, "bbb");
        rdb.ListSetByIndex("listcount", -1, "aaa");
        Assert.AreEqual("ggg", (string)rdb.ListLeftPop("listcount"));
        Assert.IsTrue(
          new[] { "fff", "ee" }.SequenceEqual(
            (string[])rdb.Execute("LPOP", "listcount", 2)));
        Assert.IsTrue(
          new[] { "aaa", "bbb" }.SequenceEqual(
            (string[])rdb.Execute("RPOP", "listcount", 2)));
        Assert.IsTrue(
          new[] { "cc" }.SequenceEqual((string[])rdb.Execute("RPOP", "listcount", 1)));
        Assert.IsTrue(
          new[] { "dd" }.SequenceEqual((string[])rdb.Execute("RPOP", "listcount", 123)));
        Assert.AreEqual(null, (string?)rdb.Execute("LPOP", "listcount"));
        Assert.AreEqual(null, (string?)rdb.Execute("RPOP", "listcount"));
        Assert.AreEqual(0, rdb.ListLength("listcount"));

        Assert.AreEqual(
          7,
          rdb.ListRightPush(
            "listcount",
            [
              "aa",
              "bb",
              "cc",
              "dd",
              "ee",
              "ff",
              "gg"
            ]));
        Assert.AreEqual(7, rdb.ListLength("listcount"));
        Assert.IsTrue(
          Array.Empty<RedisValue>()
            .SequenceEqual((RedisValue[])rdb.Execute("LPOP", "listcount", 0)));
        Assert.AreEqual("aa", (string)rdb.ListLeftPop("listcount"));
        Assert.IsTrue(
          new[] { "bb", "cc" }.SequenceEqual(
            (string[])rdb.Execute("LPOP", "listcount", 2)));
        Assert.IsTrue(
          new[] { "gg", "ff" }.SequenceEqual(
            (string[])rdb.Execute("RPOP", "listcount", 2)));
        Assert.IsTrue(
          new[] { "ee" }.SequenceEqual((string[])rdb.Execute("RPOP", "listcount", 1)));
        Assert.IsTrue(
          new[] { "dd" }.SequenceEqual((string[])rdb.Execute("RPOP", "listcount", 123)));
        Assert.AreEqual(null, (string?)rdb.Execute("LPOP", "listcount"));
        Assert.AreEqual(null, (string?)rdb.Execute("RPOP", "listcount"));
        Assert.AreEqual(0, rdb.ListLength("listcount"));

        _ = rdb.KeyDelete("mylist");
        Assert.AreEqual(
          4,
          rdb.ListLeftPush(
            "mylist",
            [
              "a",
              "b",
              "c",
              "d"
            ]));
        Assert.AreEqual(
          8,
          rdb.ListRightPush(
            "mylist",
            [
              0,
              1,
              2,
              3
            ]));
        Assert.IsTrue(
          new RedisValue[] { "d", "c", "b", "a", 0, 1, 2, 3 }.SequenceEqual(
            rdb.ListRange("mylist")));
        Assert.IsTrue(
          new RedisValue[] { "c", "b", "a", 0, 1, 2 }.SequenceEqual(
            rdb.ListRange("mylist", 1, -2)));

        Assert.IsTrue(rdb.KeyDelete("mylist"));
        Assert.IsFalse(rdb.KeyExists("mylist"));
        Assert.AreEqual(0, rdb.ListLength("mylist"));

        _ = rdb.KeyDelete("xlist");
        Assert.AreEqual(0, rdb.ListLeftPush("xlist", "a", When.Exists));
        Assert.AreEqual(0, rdb.ListLength("xlist"));
        Assert.AreEqual(0, rdb.ListRightPush("xlist", "a", When.Exists));
        Assert.AreEqual(0, rdb.ListLength("xlist"));
      });
  }

  [TestMethod]
  public void TestRedisRange()
  {
    Run(
      (__, rdb, db) => {
        _ = rdb.StringSet("mykey", "foo");
        Assert.AreEqual(3, rdb.StringSetRange("mykey", 0, "b"));
        Assert.AreEqual("boo", (string)rdb.StringGet("mykey"));

        _ = rdb.StringSet("mykey", "foo");
        Assert.AreEqual(3, rdb.StringSetRange("mykey", 0, ""));
        Assert.AreEqual("foo", (string)rdb.StringGet("mykey"));

        _ = rdb.StringSet("mykey", "foo");
        Assert.AreEqual(3, rdb.StringSetRange("mykey", 1, "b"));
        Assert.AreEqual("fbo", (string)rdb.StringGet("mykey"));

        _ = rdb.StringSet("mykey", "foo");
        Assert.AreEqual(7, rdb.StringSetRange("mykey", 4, "bar"));
        Assert.AreEqual("foo\0bar", (string)rdb.StringGet("mykey"));

        _ = rdb.KeyDelete("mykey");
        _ = rdb.StringIncrement("mykey", 1234);
        Assert.AreEqual(4, rdb.StringSetRange("mykey", 0, "2"));
        Assert.AreEqual("2234", (string)rdb.StringGet("mykey"));

        _ = rdb.KeyDelete("mykey");
        _ = rdb.StringIncrement("mykey", 1234);
        Assert.AreEqual(4, rdb.StringSetRange("mykey", 0, ""));
        Assert.AreEqual("1234", (string)rdb.StringGet("mykey"));

        _ = rdb.KeyDelete("mykey");
        _ = rdb.StringIncrement("mykey", 1234);
        Assert.AreEqual(4, rdb.StringSetRange("mykey", 1, "3"));
        Assert.AreEqual("1334", (string)rdb.StringGet("mykey"));

        _ = rdb.KeyDelete("mykey");
        _ = rdb.StringIncrement("mykey", 1234);
        Assert.AreEqual(6, rdb.StringSetRange("mykey", 5, "2"));
        Assert.AreEqual("1234\02", (string)rdb.StringGet("mykey"));

        _ = rdb.KeyDelete("mykey");
        Assert.IsTrue(
          Array.Empty<byte>().SequenceEqual((byte[])rdb.StringGetRange("mykey", 0, -1)));

        _ = rdb.StringSet("mykey", "Hello World");
        Assert.AreEqual("Hell", (string)rdb.StringGetRange("mykey", 0, 3));
        Assert.AreEqual("Hello World", (string)rdb.StringGetRange("mykey", 0, -1));
        Assert.AreEqual("orld", (string)rdb.StringGetRange("mykey", -4, -1));
        Assert.AreEqual("", (string)rdb.StringGetRange("mykey", 5, 3));
        Assert.AreEqual(" World", (string)rdb.StringGetRange("mykey", 5, 5000));
        Assert.AreEqual("Hello World", (string)rdb.StringGetRange("mykey", -5000, 10000));

        _ = rdb.KeyDelete("mykey");
        _ = rdb.StringIncrement("mykey", 1234);
        Assert.AreEqual("123", (string)rdb.StringGetRange("mykey", 0, 2));
        Assert.AreEqual("1234", (string)rdb.StringGetRange("mykey", 0, -1));
        Assert.AreEqual("234", (string)rdb.StringGetRange("mykey", -3, -1));
        Assert.AreEqual("", (string)rdb.StringGetRange("mykey", 5, 3));
        Assert.AreEqual("4", (string)rdb.StringGetRange("mykey", 3, 5000));
        Assert.AreEqual("1234", (string)rdb.StringGetRange("mykey", -5000, 10000));
      });
  }

  [TestMethod]
  [SuppressMessage(
    "Performance",
    "CA1861:Avoid constant arrays as arguments",
    Justification = "<Pending>")]
  public void TestRedisSet()
  {
    Run(
      (__, rdb, db) => {
        Assert.IsTrue(rdb.SetAdd("myset", "foo"));
        Assert.IsTrue(rdb.SetAdd("myset", "bar"));
        Assert.IsFalse(rdb.SetAdd("myset", "foo"));
        Assert.AreEqual(2, rdb.SetLength("myset"));
        Assert.IsTrue(rdb.SetContains("myset", "foo"));
        Assert.IsTrue(rdb.SetContains("myset", "bar"));
        Assert.AreEqual(1, (long)rdb.Execute("SISMEMBER", "myset", "foo"));
        Assert.AreEqual(0, (long)rdb.Execute("SISMEMBER", "myset", "bla"));
        Assert.IsTrue(
          new[] { 1L, 1L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bar")));
        Assert.IsTrue(
          new[] { 1L, 0L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bla")));
        Assert.IsTrue(
          new RedisValue[] { "bar", "foo" }.SequenceEqual(
            [.. rdb.SetMembers("myset").OrderBy(s => (string)s)]));

        const long FILL = 500;
        var sb = new StringBuilder();

        for (var i = 0L; i < FILL; i += 5) {
          _ = rdb.SetAdd(
            "myset",
            [
              MakeTestStringFor(i),
              MakeTestStringFor(i + 1),
              MakeTestStringFor(i + 2),
              MakeTestStringFor(i + 3),
              MakeTestStringFor(i + 4)
            ]);
        }

        Assert.AreEqual(FILL + 2, rdb.SetLength("myset"));
        Assert.IsTrue(rdb.SetContains("myset", "foo"));
        Assert.IsTrue(rdb.SetContains("myset", "bar"));
        Assert.AreEqual(1, (long)rdb.Execute("SISMEMBER", "myset", "foo"));
        Assert.AreEqual(0, (long)rdb.Execute("SISMEMBER", "myset", "bla"));
        Assert.IsTrue(
          new[] { 1L, 1L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bar")));
        Assert.IsTrue(
          new[] { 1L, 0L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bla")));

        for (var i = 0L; i < FILL; i += 5) {
          _ = rdb.SetRemove(
            "myset",
            [
              MakeTestStringFor(i),
              MakeTestStringFor(i + 1),
              MakeTestStringFor(i + 2),
              MakeTestStringFor(i + 3),
              MakeTestStringFor(i + 4)
            ]);
        }

        Assert.AreEqual(2, rdb.SetLength("myset"));
        Assert.IsTrue(rdb.SetContains("myset", "foo"));
        Assert.IsTrue(rdb.SetContains("myset", "bar"));
        Assert.AreEqual(1, (long)rdb.Execute("SISMEMBER", "myset", "foo"));
        Assert.AreEqual(0, (long)rdb.Execute("SISMEMBER", "myset", "bla"));
        Assert.IsTrue(
          new[] { 1L, 1L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bar")));
        Assert.IsTrue(
          new[] { 1L, 0L }.SequenceEqual(
            (long[])rdb.Execute("SMISMEMBER", "myset", "foo", "bla")));

        return;

        string MakeTestStringFor(long i)
        {
          _ = sb.Clear();
          for (var j = 0; j < 1024; ++j) {
            _ = sb.Append(i);
            _ = sb.Append('-');
          }

          return sb.ToString();
        }
      });
  }

  [TestMethod]
  public void TestRedisStrLen()
  {
    Run(
      (__, rdb, db) => {
        Assert.AreEqual(0, rdb.StringLength("non-existent"));
        _ = rdb.StringDecrement("int", 555);
        Assert.AreEqual(4, rdb.StringLength("int"));
        _ = rdb.StringSet("str", "foozzz0123456789 baz");
        Assert.AreEqual(20, rdb.StringLength("str"));
      });
  }
}

using System;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json.Serialization;

using JetBrains.Annotations;

using MessagePack;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

[TestClass]
public class SerializerTests
{
  [TestMethod]
  public void TestBool()
  {
    var serializer = new PlaneBoolSerializer();
    serializer.TestEqual(true);
    serializer.TestEqual(false);
  }

  [TestMethod]
  public void TestByte()
  {
    var serializer = new PlaneByteSerializer();
    serializer.TestEqual((byte)0);
    serializer.TestEqual((byte)1);
    serializer.TestEqual(byte.MaxValue);
    serializer.TestEqual(byte.MinValue);
  }

  [TestMethod]
  public void TestChar()
  {
    var serializer = new PlaneCharSerializer();
    serializer.TestEqual((char)0);
    serializer.TestEqual((char)1);
    serializer.TestEqual(char.MaxValue);
    serializer.TestEqual(char.MinValue);
  }

  [TestMethod]
  public void TestDateTime()
  {
    var serializer = new PlaneDateTimeSerializer();
    serializer.TestEqual(DateTime.Now);
    serializer.TestEqual(DateTime.Today);
    serializer.TestEqual(DateTime.UnixEpoch);
    serializer.TestEqual(DateTime.MinValue);
    serializer.TestEqual(DateTime.MaxValue);
  }

  [TestMethod]
  public void TestDecimal()
  {
    var serializer = new PlaneDecimalSerializer();
    serializer.TestEqual(0);
    serializer.TestEqual(new decimal(0.001));
    serializer.TestEqual(new decimal(2.2));
    serializer.TestEqual(decimal.MinValue);
    serializer.TestEqual(decimal.MaxValue);
  }

  [TestMethod]
  public void TestDouble()
  {
    var serializer = new PlaneDoubleSerializer();
    serializer.TestEqual(double.MinValue);
    serializer.TestEqual(double.MaxValue);
    serializer.TestEqual(double.NaN);
    serializer.TestEqual(double.Epsilon);
    serializer.TestEqual(double.NegativeInfinity);
    serializer.TestEqual(double.PositiveInfinity);
    serializer.TestEqual(0.0);
    serializer.TestEqual(-0.0);
    serializer.TestEqual(1.0);
    serializer.TestEqual(-1.0);
  }

  [TestMethod]
  public void TestFloat()
  {
    var serializer = new PlaneFloatSerializer();
    serializer.TestEqual(float.MinValue);
    serializer.TestEqual(float.MaxValue);
    serializer.TestEqual(float.NaN);
    serializer.TestEqual(float.Epsilon);
    serializer.TestEqual(float.NegativeInfinity);
    serializer.TestEqual(float.PositiveInfinity);
    serializer.TestEqual(0.0f);
    serializer.TestEqual(-0.0f);
    serializer.TestEqual(1.0f);
    serializer.TestEqual(-1.0f);
  }

  [TestMethod]
  public void TestGuid()
  {
    var serializer = new PlaneGuidSerializer();

    serializer.TestEqual(Guid.Empty);
    serializer.TestEqual(Guid.NewGuid());
    serializer.TestEqual(Guid.Empty);
  }

  [TestMethod]
  public void TestInt16()
  {
    var serializer = new PlaneInt16Serializer();
    serializer.TestEqual((short)0);
    serializer.TestEqual((short)1);
    serializer.TestEqual((short)-1);
    serializer.TestEqual(short.MaxValue);
    serializer.TestEqual(short.MinValue);
  }

  [TestMethod]
  public void TestInt32()
  {
    var serializer = new PlaneInt32Serializer();
    serializer.TestEqual(0);
    serializer.TestEqual(1);
    serializer.TestEqual(-1);
    serializer.TestEqual(int.MaxValue);
    serializer.TestEqual(int.MinValue);
  }

  [TestMethod]
  public void TestInt64()
  {
    var serializer = new PlaneInt64Serializer();
    serializer.TestEqual(0);
    serializer.TestEqual(1);
    serializer.TestEqual(-1);
    serializer.TestEqual(long.MaxValue);
    serializer.TestEqual(long.MinValue);
  }

  [TestMethod]
  public void TestJson()
  {
    var serializer = new PlaneJsonSerializer<TestObject>();
    serializer.TestEqual(new TestObject());
    serializer.TestEqual(new TestObject { Bar = 1 });
    serializer.TestEqual(
      new TestObject {
        Bar = -2,
        Baz = true
      });
    serializer.TestEqual(
      new TestObject {
        Bar = 3,
        Foo = "hello world"
      });
    serializer.TestEqual(
      new TestObject {
        Bar = -4,
        Foo = "hello world",
        Baz = true
      });
  }

  [TestMethod]
  public void TestMessagePack()
  {
    var serializer = new PlaneMessagePackSerializer<TestObject>();
    serializer.TestEqual(new TestObject());
    serializer.TestEqual(new TestObject { Bar = 1 });
    serializer.TestEqual(
      new TestObject {
        Bar = -2,
        Baz = true
      });
    serializer.TestEqual(
      new TestObject {
        Bar = 3,
        Foo = "hello world"
      });
    serializer.TestEqual(
      new TestObject {
        Bar = -4,
        Foo = "hello world",
        Baz = true
      });
  }

  [TestMethod]
  public void TestNullableObject()
  {
    var serializer =
      new PlaneNullableObjectSerializer<string>(new PlaneStringSerializer());
    serializer.TestEqual(null);
    serializer.TestEqual(string.Empty);
    serializer.TestEqual("abc");
    serializer.TestEqual("äÖß☃");
    var sb = new StringBuilder();
    for (var i = 0; i < 10; ++i) {
      _ = sb.Append("äÖß☃");
    }

    serializer.TestEqual(sb.ToString());

    for (var i = 0; i < 10000; ++i) {
      _ = sb.Append("äÖß☃");
    }

    serializer.TestEqual(sb.ToString());
    serializer.TestEqual(sb.ToString().Normalize(NormalizationForm.FormKD));
  }

  [TestMethod]
  public void TestNullablePOD()
  {
    var serializer =
      new PlaneNullablePlainSerializer<ushort>(new PlaneUInt16Serializer());
    serializer.TestEqual(null);
    serializer.TestEqual((ushort)0);
    serializer.TestEqual((ushort)1);
    serializer.TestEqual(ushort.MaxValue);
    serializer.TestEqual(ushort.MinValue);
  }

  [TestMethod]
  public void TestPassthrough()
  {
    var serializer = new PlanePassthroughSerializer();
    serializer.TestEqualArray([]);
    serializer.TestEqualArray("test"u8.ToArray());
    serializer.TestEqualArray(new byte[] { 0 });
  }

  [TestMethod]
  public void TestSByte()
  {
    var serializer = new PlaneSByteSerializer();
    serializer.TestEqual((sbyte)0);
    serializer.TestEqual((sbyte)1);
    serializer.TestEqual(sbyte.MaxValue);
    serializer.TestEqual(sbyte.MinValue);
  }

  [TestMethod]
  public void TestString()
  {
    var serializer = new PlaneStringSerializer();
    _ = Assert.ThrowsException<NullReferenceException>(() => serializer.Serialize(null!));
    serializer.TestEqual(string.Empty);
    serializer.TestEqual("abc");
    serializer.TestEqual("äÖß☃");
    var sb = new StringBuilder();
    for (var i = 0; i < 10; ++i) {
      _ = sb.Append("äÖß☃");
    }

    serializer.TestEqual(sb.ToString());

    for (var i = 0; i < 10000; ++i) {
      _ = sb.Append("äÖß☃");
    }

    serializer.TestEqual(sb.ToString());
    serializer.TestEqual(sb.ToString().Normalize(NormalizationForm.FormKD));
  }

  [TestMethod]
  public void TestTimeSpan()
  {
    var serializer = new PlaneTimeSpanSerializer();
    serializer.TestEqual(TimeSpan.Zero);
    serializer.TestEqual(TimeSpan.FromDays(2));
    serializer.TestEqual(TimeSpan.MinValue);
    serializer.TestEqual(TimeSpan.MaxValue);
  }

  [TestMethod]
  public void TestUInt16()
  {
    var serializer = new PlaneUInt16Serializer();
    serializer.TestEqual((ushort)0);
    serializer.TestEqual((ushort)1);
    serializer.TestEqual(ushort.MaxValue);
    serializer.TestEqual(ushort.MinValue);
  }

  [TestMethod]
  public void TestUInt32()
  {
    var serializer = new PlaneUInt32Serializer();
    serializer.TestEqual(0U);
    serializer.TestEqual(1U);
    serializer.TestEqual(uint.MaxValue);
    serializer.TestEqual(uint.MinValue);
  }

  [TestMethod]
  public void TestUInt64()
  {
    var serializer = new PlaneUInt64Serializer();
    serializer.TestEqual(0U);
    serializer.TestEqual(1U);
    serializer.TestEqual(ulong.MaxValue);
    serializer.TestEqual(ulong.MinValue);
  }

  [MessagePackObject]
  [PublicAPI]
  public class TestObject : IEquatable<TestObject>
  {
    [JsonInclude]
    [Key(2)]
    public long Bar;

    [JsonInclude]
    [Key(3)]
    public bool Baz;

    [JsonInclude]
    [Key(1)]
    public string Foo = string.Empty;

    public bool Equals(TestObject? other)
    {
      return other is not null &&
             (ReferenceEquals(this, other) ||
              (Foo == other.Foo && Bar == other.Bar && Baz == other.Baz));
    }

    public override bool Equals(object? obj)
    {
      return obj is not null &&
             (ReferenceEquals(this, obj) ||
              (obj.GetType() == GetType() && Equals((TestObject)obj)));
    }

    [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
    [SuppressMessage(
      "CodeQuality",
      "IDE0079:Remove unnecessary suppression",
      Justification = "jb")]
    public override int GetHashCode()
    {
      return HashCode.Combine(Foo, Bar, Baz);
    }
  }
}

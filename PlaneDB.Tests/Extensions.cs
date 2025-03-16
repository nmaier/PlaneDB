using System;
using System.IO;
using System.Linq;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

internal static class Extensions
{
  internal static Stream AsMemoryStream(this string s)
  {
    return new MemoryStream(Encoding.UTF8.GetBytes(s), false);
  }

  internal static void TestEqual<T>(this IPlaneSerializer<T> serializer, T val)
  {
    var s = serializer.Serialize(val);
    Assert.AreEqual(serializer.Deserialize(s.AsSpan()), val);
  }

  internal static void TestEqualArray<T>(this IPlaneSerializer<T[]> serializer, T[] val)
    where T : struct
  {
    var s = serializer.Serialize(val);
    Assert.IsTrue(serializer.Deserialize(s.AsSpan()).SequenceEqual(val));
  }
}

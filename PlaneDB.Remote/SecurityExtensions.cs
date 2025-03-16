using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace NMaier.PlaneDB;

internal static class SecurityExtensions
{
  internal static byte[] AuthHash(this string str)
  {
    return AuthHash(Encoding.UTF8.GetBytes(str));
  }

  internal static byte[] AuthHash(this byte[] data)
  {
    return SHA512.HashData(data);
  }

  [MethodImpl(MethodImplOptions.NoOptimization)]
  internal static bool ConstantTimeEquals(this byte[] a, byte[] b)
  {
    var diff = a.Length ^ b.Length;
    var len = Math.Min(b.Length, a.Length);
    for (var i = 0; i < len; i++) {
      diff |= a[i] ^ b[i];
    }

    return diff == 0;
  }
}

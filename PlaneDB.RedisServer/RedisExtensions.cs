using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Extensions to server PlaneDBs over the redis protocol
/// </summary>
[PublicAPI]
public static class RedisExtensions
{
  private static readonly Random shuffler = new();

  /// <summary>
  ///   Serve a db using the redis protocol
  /// </summary>
  /// <param name="db">Database to serve</param>
  /// <param name="options">Server options</param>
  /// <param name="token">Cancellation token to stop serving</param>
  /// <returns></returns>
  public static IPlaneDBRemote ServeRedis(
    this IPlaneDB<byte[], byte[]> db,
    PlaneDBRemoteOptions options,
    CancellationToken token)
  {
    var server = new RedisServer(db, options);

    return server.Serve(token);
  }

  /// <summary>
  ///   Serve a db using the redis protocol
  /// </summary>
  /// <param name="db">Database to serve</param>
  /// <param name="options">Server options</param>
  /// <param name="token">Cancellation token to stop serving</param>
  /// <returns></returns>
  public static IPlaneDBRemote ServeRedis<TKey, TValue>(
    this TypedPlaneDB<TKey, TValue> db,
    PlaneDBRemoteOptions options,
    CancellationToken token) where TKey : notnull
  {
    return ServeRedis(db.BaseDB, options, token);
  }

  private static T[] Shuffle<T>(this T[] array)
  {
    var n = array.Length;
    while (n > 1) {
      var k = shuffler.Next(n--);
      (array[n], array[k]) = (array[k], array[n]);
    }

    return array;
  }

  internal static T[] Shuffled<T>(this IEnumerable<T> sequence)
  {
    return sequence switch {
      T[] arr => Shuffle(arr.AsSpan().ToArray()),
      _ => Shuffle(sequence.ToArray())
    };
  }

  /// <summary>
  ///   String matching, the redis way
  /// </summary>
  /// <param name="str">String to match</param>
  /// <param name="pattern">Pattern to match</param>
  /// <returns></returns>
  [MethodImpl(Constants.SHORT_METHOD)]
  internal static bool StringMatch(
    this ReadOnlySpan<byte> str,
    ReadOnlySpan<byte> pattern)
  {
    return StringMatchLen(str, str.Length, pattern, pattern.Length);
  }

  /// <summary>
  ///   String matching, the redis way
  /// </summary>
  /// <param name="str">String to match</param>
  /// <param name="stringLen">String length</param>
  /// <param name="pattern">Pattern to match</param>
  /// <param name="patLen">Length of pattern</param>
  /// <returns></returns>
  private static bool StringMatchLen(
    this ReadOnlySpan<byte> str,
    int stringLen,
    ReadOnlySpan<byte> pattern,
    int patLen)
  {
    var pp = 0;
    while (patLen > 0 && stringLen > 0) {
      switch (pattern[pp]) {
        case (byte)'*':
          while (patLen > 1 && pattern[pp + 1] == '*') {
            pp++;
            patLen--;
          }

          if (patLen == 1) {
            return true;
          }

          while (stringLen > 0) {
            if (StringMatchLen(str, stringLen, pattern[(pp + 1)..], patLen - 1)) {
              return true;
            }

            str = str[1..];
            stringLen--;
          }

          return false;

        case (byte)'?':
          str = str[1..];
          stringLen--;

          break;

        case (byte)'[': {
          pp++;
          patLen--;
          var not = patLen > 0 && pattern[pp] == '^';
          if (not) {
            pp++;
            patLen--;
          }

          var match = false;
          for (;;) {
            if (patLen >= 2 && pattern[pp] == '\\') {
              pp++;
              patLen--;
              if (pattern[pp] == str[0]) {
                match = true;
              }
            }
            else if (patLen > 0 && pattern[pp] == ']') {
              break;
            }
            else {
              switch (patLen) {
                case >= 3 when pattern[pp + 1] == '-': {
                  var start = pattern[pp];
                  var end = pattern[pp + 2];
                  var c = str[0];
                  if (start > end) {
                    (start, end) = (end, start);
                  }

                  pp += 2;
                  patLen -= 2;
                  if (c >= start && c <= end) {
                    match = true;
                  }

                  break;
                }
                case > 0 when pattern[pp] == str[0]:
                  match = true;

                  break;
              }
            }

            pp++;
            patLen--;
          }

          if (not) {
            match = !match;
          }

          if (!match) {
            return false;
          }

          str = str[1..];
          stringLen--;

          break;
        }
        case (byte)'\\':
          if (patLen >= 2) {
            pp++;
            patLen--;
          }

          goto default;

        default:
          if (pattern[pp] != str[0]) {
            return false;
          }

          str = str[1..];
          stringLen--;

          break;
      }

      pp++;
      patLen--;
      if (stringLen != 0) {
        continue;
      }

      while (patLen > 0 && pattern[pp] == '*') {
        ++pp;
        patLen--;
      }

      break;
    }

    return patLen == 0 && stringLen == 0;
  }
}

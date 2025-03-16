using System;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Non-Cryptographic secure Randomness generator for protocol canaries
/// </summary>
/// <remarks>
///   Based on C#'s current random generator, which is based on a modified version of Donald E. Knuth's subtractive
///   random number generator algorithm. For more information, see D. E. Knuth. The Art of Computer Programming, Volume 2:
///   Seminumerical Algorithms. Addison-Wesley, Reading, MA, third edition, 1997.
/// </remarks>
[PublicAPI]
public class PlaneProtocolRandom
{
  private readonly int[] arr = new int[56];
  private int next;
  private int nextPrime;

  /// <summary>
  ///   Construct generator based on (common) seed
  /// </summary>
  /// <param name="seed">Seed to use</param>
  public PlaneProtocolRandom(int seed)
  {
    var ii = 0;
    var sub = seed == int.MinValue ? int.MaxValue : Math.Abs(seed);
    var mj = 161803398 - sub;
    arr[55] = mj;
    var mk = 1;
    for (var i = 1; i < 55; i++) {
      if ((ii += 21) >= 55) {
        ii -= 55;
      }

      arr[ii] = mk;
      mk = mj - mk;
      if (mk < 0) {
        mk += int.MaxValue;
      }

      mj = arr[ii];
    }

    for (var j = 1; j < 5; j++) {
      for (var k = 1; k < 56; k++) {
        var num5 = k + 30;
        if (num5 >= 55) {
          num5 -= 55;
        }

        arr[k] -= arr[1 + num5];
        if (arr[k] < 0) {
          arr[k] += int.MaxValue;
        }
      }
    }

    nextPrime = 21;
  }

  /// <summary>
  ///   Get the next random number (canary)
  /// </summary>
  /// <returns>A random int</returns>
  public int Next()
  {
    var tmpNext = next;
    var tmpNextPrime = nextPrime;
    if (++tmpNext >= 56) {
      tmpNext = 1;
    }

    if (++tmpNextPrime >= 56) {
      tmpNextPrime = 1;
    }

    var num = arr[tmpNext] - arr[tmpNextPrime];
    if (num == int.MaxValue) {
      num--;
    }

    if (num < 0) {
      num += int.MaxValue;
    }

    arr[tmpNext] = num;
    next = tmpNext;
    nextPrime = tmpNextPrime;

    return num;
  }
}

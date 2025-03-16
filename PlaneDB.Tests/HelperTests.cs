using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

[TestClass]
public class HelperTests
{
  [TestMethod]
  public void TestBloom()
  {
    var bloom = new BloomFilter(10, 0.1);
    for (var i = 0; i < 20; i += 2) {
      bloom.Add(
      [
        (byte)i
      ]);
    }

    for (var i = 0; i < 20; i++) {
      Assert.AreEqual(
        i % 2 == 0,
        bloom.ContainsMaybe(
          new BloomFilter.Hashes(
          [
            (byte)i
          ])),
        i.ToString());
    }

    bloom = new BloomFilter(bloom.ToArray(), true);

    for (var i = 0; i < 20; i++) {
      Assert.AreEqual(
        i % 2 == 0,
        bloom.ContainsMaybe(
          new BloomFilter.Hashes(
          [
            (byte)i
          ])),
        i.ToString());
    }
  }

  [TestMethod]
  public void TestEnumerateSortedUniquely()
  {
    var expected = new[] { 0, 1, 2, 3, 4, 5, 6, 7 };
    var vectors = new[] { [
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ],
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ]
      ], [
        [
          0,
          2,
          4,
          6
        ],
        [
          0,
          1,
          3,
          5,
          7
        ]
      ], [
        [
          4,
          5,
          6,
          7
        ],
        [
          0,
          1,
          2,
          3
        ]
      ], [
        [
          3,
          4,
          5,
          6,
          7
        ],
        [
          0,
          1,
          2,
          3
        ]
      ], [
        [
          4,
          5,
          6,
          7
        ],
        [
          0,
          1,
          2,
          3,
          4
        ]
      ], [
        [
          0,
          1,
          2,
          3
        ],
        [
          4,
          5,
          6,
          7
        ]
      ], [
        [
          0,
          1,
          2,
          3,
          4
        ],
        [
          4,
          5,
          6,
          7
        ]
      ], [
        [
          0,
          1,
          2,
          3
        ],
        [
          3,
          4,
          5,
          6,
          7
        ]
      ], [
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ],
        []
      ], [
        [],
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ]
      ], [
        [
          0,
          2,
          4,
          6
        ],
        [
          0,
          1,
          3,
          7
        ],
        [
          0,
          1,
          5,
          7
        ]
      ], [
        [
          0,
          6
        ],
        [
          2,
          4
        ],
        [
          0,
          1,
          5,
          7
        ],
        [
          0,
          1,
          3,
          7
        ]
      ], [
        [
          0,
          4
        ],
        [
          2,
          6
        ],
        [
          0,
          1,
          3,
          7
        ],
        [
          0,
          1,
          5,
          7
        ]
      ], [
        [],
        [],
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ]
      ], [
        [],
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ],
        []
      ],
      new IEnumerable<int>[] { [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7
        ],
        [],
        []
      }
    };
    foreach (var (vector, i) in vectors.Select((v, i) => (v, i))) {
      var merged = vector.EnumerateSortedUniquely().ToArray();
      Assert.IsTrue(
        merged.AsSpan().SequenceEqual(expected),
        $"{i} {string.Join(", ", merged)}");
    }
  }

  [TestMethod]
  public void TestExitStack()
  {
    {
      using var stack = new ExitStack();
      for (var i = 0; i < 10; i++) {
        _ = stack.Register(new Throwable());
      }
    }

    var ex = Assert.ThrowsException<AggregateException>(
      () => {
        {
          using var stack = new ExitStack();
          for (var i = 0; i < 10; i++) {
            _ = stack.Register(new Throwable(i % 2 == 0));
          }
        }
      });
    Assert.AreEqual(5, ex.InnerExceptions.Count, "All exceptions aggregated");
  }

  [TestMethod]
  public void TestXXHash()
  {
    var vector = new KeyValuePair<string, int>[] {
      new("", 46947589),
      new("abc", 852579327),
      new("ABC", -2140066091),
      new("\n\n\a", 1334766520),
      new("\n\0\0", -2081853664),
      new("äbc", -71767665),
      new("ábè", -577204069)
    };
    foreach (var (key, value) in vector) {
      Assert.AreEqual(
        value,
        InternalExtensions.ComputeXXHash(Encoding.UTF8.GetBytes(key)));
    }
  }

  [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
  private sealed class Throwable(bool shouldThrow = false) : IDisposable
  {
    public void Dispose()
    {
      if (shouldThrow) {
        throw new Exception("I was asked to");
      }
    }
  }
}

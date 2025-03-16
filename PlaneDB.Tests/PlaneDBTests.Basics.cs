using System;
using System.IO;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace NMaier.PlaneDB.Tests;

public sealed partial class PlaneDBTests
{
  [TestMethod]
  public void TestAlreadyLocked()
  {
    _ = Assert.ThrowsExactly<PlaneDBAlreadyLockedException>(
      () => {
        using (new StringPlaneDB(
                 new DirectoryInfo(testDB),
                 planeOptions.WithOpenMode(PlaneOpenMode.ReadWrite)))
        using (new StringPlaneDB(
                 new DirectoryInfo(testDB),
                 planeOptions.WithOpenMode(PlaneOpenMode.ReadWrite))) {
        }
      });
  }

  [TestMethod]
  public void TestOptions()
  {
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.FlushJournalAfterNumberOfWrites(-1));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.FlushJournalAfterNumberOfWrites(int.MinValue));
    _ = planeOptions.FlushJournalAfterNumberOfWrites(0);
    _ = planeOptions.FlushJournalAfterNumberOfWrites(int.MaxValue);

    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheCapacity(0));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheCapacity(-1));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheCapacity(int.MinValue));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheCapacity(int.MaxValue));
    _ = planeOptions.WithBlockCacheCapacity(1);
    _ = planeOptions.WithBlockCacheCapacity(100_000);

    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheByteSize(0));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheByteSize(-1));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithBlockCacheByteSize(int.MinValue));
    _ = planeOptions.WithBlockCacheByteSize(1);
    _ = planeOptions.WithBlockCacheByteSize(100_000);
    _ = planeOptions.WithBlockCacheByteSize(int.MaxValue);

    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithLevel0TargetSize((PlaneLevel0TargetSize)1));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithLevel0TargetSize((PlaneLevel0TargetSize)1024));
    _ = planeOptions.WithLevel0TargetSize(PlaneLevel0TargetSize.DefaultSize);
    _ = planeOptions.WithLevel0TargetSize(PlaneLevel0TargetSize.DoubleSize);
    _ = planeOptions.WithLevel0TargetSize(PlaneLevel0TargetSize.QuadrupleSize);
    _ = planeOptions.WithLevel0TargetSize(
      (PlaneLevel0TargetSize)((uint)PlaneLevel0TargetSize.QuadrupleSize * 2));

    _ = planeOptions.WithKeyCacheMode(PlaneKeyCacheMode.NoKeyCaching);
    _ = planeOptions.WithKeyCacheMode(PlaneKeyCacheMode.AutoKeyCaching);
    _ = planeOptions.WithKeyCacheMode(PlaneKeyCacheMode.ForceKeyCaching);
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithKeyCacheMode((PlaneKeyCacheMode)int.MaxValue));

    _ = Assert.ThrowsException<InvalidOperationException>(
      () => planeOptions.WithOpenMode(PlaneOpenMode.Repair));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithOpenMode((PlaneOpenMode)int.MinValue));
    _ = Assert.ThrowsException<ArgumentOutOfRangeException>(
      () => planeOptions.WithOpenMode((PlaneOpenMode)int.MaxValue));
    _ = planeOptions.WithOpenMode(PlaneOpenMode.CreateReadWrite);
    _ = planeOptions.WithOpenMode(PlaneOpenMode.ExistingReadWrite);
    _ = planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly);
    _ = planeOptions.WithOpenMode(PlaneOpenMode.Packed);
    _ = planeOptions.WithOpenMode(PlaneOpenMode.ReadOnly);

    _ = Assert.ThrowsException<ArgumentException>(
      () => planeOptions.UsingTablespace(null!));

    _ = Assert.ThrowsException<ArgumentException>(
      () => planeOptions.UsingTablespace(string.Empty));

    _ = Assert.ThrowsException<ArgumentException>(
      () => planeOptions.UsingTablespace("#"));

    _ = Assert.ThrowsException<ArgumentException>(
      () => planeOptions.UsingTablespace("a 1"));

    _ = Assert.ThrowsException<ArgumentException>(
      () => planeOptions.UsingTablespace("ä"));

    _ = planeOptions.UsingTablespace("abc");
    _ = planeOptions.UsingTablespace("abc01");
    _ = planeOptions.UsingTablespace("1");
    _ = planeOptions.UsingTablespace("___1");
    _ = planeOptions.UsingTablespace("___1--");
    _ = planeOptions.UsingTablespace("-");
  }
}

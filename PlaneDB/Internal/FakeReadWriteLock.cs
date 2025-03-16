using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class FakeReadWriteLock : IPlaneReadWriteLock
{
  [MethodImpl(Constants.SHORT_METHOD)]
  public void EnterReadLock(out bool taken)
  {
    taken = false;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void EnterUpgradeableReadLock(out bool taken)
  {
    taken = false;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void EnterWriteLock(out bool taken)
  {
    taken = false;
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void ExitReadLock()
  {
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void ExitUpgradeableReadLock()
  {
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public void ExitWriteLock()
  {
  }
}

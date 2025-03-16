using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class ReadOnlyLock : IPlaneReadWriteLock
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

  public void EnterWriteLock(out bool taken)
  {
    throw new PlaneDBReadOnlyException();
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
    throw new PlaneDBReadOnlyException();
  }
}

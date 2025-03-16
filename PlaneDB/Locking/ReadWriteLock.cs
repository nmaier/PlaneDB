using System.Threading;

namespace NMaier.PlaneDB;

internal sealed class ReadWriteLock : ReaderWriterLockSlim, IPlaneReadWriteLock
{
  internal ReadWriteLock() : base(LockRecursionPolicy.NoRecursion)
  {
  }

  public void EnterReadLock(out bool taken)
  {
    try {
      EnterReadLock();
      taken = true;
    }
    catch (LockRecursionException) {
      taken = false;
    }
  }

  public void EnterUpgradeableReadLock(out bool taken)
  {
    try {
      EnterUpgradeableReadLock();
      taken = true;
    }
    catch (LockRecursionException) {
      taken = false;
    }
  }

  public void EnterWriteLock(out bool taken)
  {
    try {
      EnterWriteLock();
      taken = true;
    }
    catch (LockRecursionException) {
      taken = false;
    }
  }
}

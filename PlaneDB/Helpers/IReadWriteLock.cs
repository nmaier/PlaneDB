using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace NMaier.PlaneDB
{
  internal interface IReadWriteLock : IDisposable
  {
    void EnterReadLock();
    void EnterUpgradeableReadLock();

    void EnterWriteLock();
    void ExitReadLock();
    void ExitUpgradeableReadLock();
    void ExitWriteLock();
  }

  internal sealed class ReadWriteLock : ReaderWriterLockSlim, IReadWriteLock
  {
    internal ReadWriteLock() : base(LockRecursionPolicy.SupportsRecursion)
    {
    }
  }

  internal sealed class FakeReadWriteLock : IReadWriteLock
  {
    public void Dispose()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnterReadLock()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnterUpgradeableReadLock()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EnterWriteLock()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ExitReadLock()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ExitUpgradeableReadLock()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ExitWriteLock()
    {
    }
  }
}
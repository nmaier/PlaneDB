using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB
{
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
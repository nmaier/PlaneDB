using System.Threading;

namespace NMaier.PlaneDB
{
  internal sealed class ReadWriteLock : ReaderWriterLockSlim, IReadWriteLock
  {
    internal ReadWriteLock() : base(LockRecursionPolicy.SupportsRecursion)
    {
    }
  }
}
using System.Threading;

namespace NMaier.PlaneDB
{
  /// <summary>
  ///   General locking interface.
  ///   Each lock can have either at any time:
  ///   <list type="bullet">
  ///     <item>
  ///       <description>many active readers and no upgradble readers and no writers</description>
  ///     </item>
  ///     <item>
  ///       <description>many active readers and one upgradable reader and no writers</description>
  ///     </item>
  ///     <item>
  ///       <description>no active readers and no upgradable reader and one writer</description>
  ///     </item>
  ///   </list>
  /// </summary>
  /// <seealso cref="ReaderWriterLockSlim" />
  /// <remarks>Implementations must support recursion, and must allow sharing between db instances</remarks>
  public interface IReadWriteLock
  {
    /// <summary>
    ///   Enter the lock in read mode.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.EnterReadLock" />
    void EnterReadLock();

    /// <summary>
    ///   Enter the lock in upgradable read mode.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.EnterUpgradeableReadLock" />
    void EnterUpgradeableReadLock();

    /// <summary>
    ///   Enter the lock in write mode.
    ///   Read locks cannot be re-entered in write mode, but upgradable locks can.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.EnterWriteLock" />
    void EnterWriteLock();

    /// <summary>
    ///   Exit the locks read mode.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.ExitReadLock" />
    void ExitReadLock();

    /// <summary>
    ///   Exit the locks upgradable read mode.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.ExitUpgradeableReadLock" />
    void ExitUpgradeableReadLock();

    /// <summary>
    ///   Exit the locks write mode.
    /// </summary>
    /// <seealso cref="ReaderWriterLockSlim.ExitWriteLock" />
    void ExitWriteLock();
  }
}
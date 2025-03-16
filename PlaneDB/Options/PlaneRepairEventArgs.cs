using System;
using System.IO;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   In repair mode, events with these arguments are raised whenever a repair was necessary
/// </summary>
[PublicAPI]
public class PlaneRepairEventArgs : EventArgs
{
  /// <summary>
  ///   Affected file that was repaired (replaced)
  /// </summary>
  public readonly FileInfo File;

  /// <summary>
  ///   Reason the file was affected
  /// </summary>
  public readonly Exception Reason;

  internal PlaneRepairEventArgs(FileInfo file, Exception reason)
  {
    File = file;
    Reason = reason;
  }
}

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Open mode with which to open a PlaneDB/PlaneSet
/// </summary>
[PublicAPI]
public enum PlaneOpenMode
{
  /// <summary>
  ///   Open a packed database (always read-only)
  /// </summary>
  Packed,

  /// <summary>
  ///   Open a database read-only. A database can be opened read-only many times, as long as there is no read-write instance
  ///   open for the same location
  /// </summary>
  ReadOnly,

  /// <summary>
  ///   Create a new database, fail if there is an existing database in the same location
  /// </summary>
  CreateReadWrite,

  /// <summary>
  ///   Open an existing database, fail if the database does not exist
  /// </summary>
  ExistingReadWrite,

  /// <summary>
  ///   Open a database in read-write mode. If the database does not exist, it will be created
  /// </summary>
  ReadWrite,

  /// <summary>
  ///   Open a database in repair mode. You usually do NOT want to do this
  /// </summary>
  Repair
}

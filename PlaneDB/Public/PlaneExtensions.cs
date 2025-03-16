using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Various helpful type extensions
/// </summary>
[PublicAPI]
public static class PlaneExtensions
{
  /// <summary>
  ///   Enumerate a range of a byte[]-based DB.
  /// </summary>
  /// <param name="db">DB to enumerate</param>
  /// <param name="from">Enumerate keys from (inclusive)</param>
  /// <param name="to">Enumerate keys to (inclusive)</param>
  /// <returns>Enumerable of the key-values in this DB within the specified range</returns>
  /// <remarks>Result will be ordered according to <see cref="PlaneByteArrayComparer" /></remarks>
  public static IEnumerable<KeyValuePair<byte[], byte[]>> Range(
    this IPlaneDB<byte[], byte[]> db,
    byte[] from,
    byte[] to)
  {
    return db.Where(kv => PlaneByteArrayComparer.Default.Compare(from, kv.Key) <= 0)
      .TakeWhile(kv => PlaneByteArrayComparer.Default.Compare(to, kv.Key) >= 0);
  }

  /// <summary>
  ///   Enumerate a range of a DB with the default comparer for the key type.
  /// </summary>
  /// <param name="db">DB to enumerate</param>
  /// <param name="from">Enumerate keys from (inclusive)</param>
  /// <param name="to">Enumerate keys to (inclusive)</param>
  /// <returns>Enumerable of the key-values in this DB within the specified range</returns>
  /// <remarks>Result order is unspecified</remarks>
  [MethodImpl(Constants.SHORT_METHOD)]
  public static IEnumerable<KeyValuePair<TKey, TValue>> Range<TKey, TValue>(
    this IPlaneDB<TKey, TValue> db,
    TKey from,
    TKey to) where TKey : notnull
  {
    return Range(db, from, to, Comparer<TKey>.Default);
  }

  /// <summary>
  ///   Enumerate a range of a DB using the provided comparer.
  /// </summary>
  /// <param name="db">DB to enumerate</param>
  /// <param name="from">Enumerate keys from (inclusive)</param>
  /// <param name="to">Enumerate keys to (inclusive)</param>
  /// <param name="comparer">Compare with this comparer</param>
  /// <returns>Enumerable of the key-values in this DB within the specified range</returns>
  /// <remarks>Result order is unspecified</remarks>
  public static IEnumerable<KeyValuePair<TKey, TValue>> Range<TKey, TValue>(
    this IPlaneDB<TKey, TValue> db,
    TKey from,
    TKey to,
    IComparer<TKey> comparer) where TKey : notnull
  {
    return db.Where(
      kv => comparer.Compare(from, kv.Key) <= 0 && comparer.Compare(to, kv.Key) >= 0);
  }

  /// <summary>
  ///   Writes a PlaneDB to a packed PlaneDB file, using the same options as the PlaneDB for the output
  /// </summary>
  /// <param name="db">Database to pack</param>
  /// <param name="file">Output file</param>
  public static void WriteToPack(this IPlaneBase db, FileInfo file)
  {
    WriteToPack(db, file, db.Options);
  }

  /// <summary>
  ///   Writes a PlaneDB to a packed PlaneDB file
  /// </summary>
  /// <param name="db">Database to pack</param>
  /// <param name="file">Output file</param>
  /// <param name="options">Options to use for the packed file</param>
  public static void WriteToPack(this IPlaneBase db, FileInfo file, PlaneOptions options)
  {
    using var fs = new FileStream(
      file.FullName,
      FileMode.CreateNew,
      FileAccess.Write,
      FileShare.None,
      short.MaxValue);
    WriteToPack(db, fs, options);
  }

  /// <summary>
  ///   Writes a PlaneDB to a packed PlaneDB file, using the same options as the PlaneDB for the output
  /// </summary>
  /// <param name="db">Database to pack</param>
  /// <param name="stream">Output stream</param>
  public static void WriteToPack(this IPlaneBase db, Stream stream)
  {
    WriteToPack(db, stream, db.Options);
  }

  /// <summary>
  ///   Writes a PlaneDB to a packed PlaneDB file
  /// </summary>
  /// <param name="db">Database to pack</param>
  /// <param name="stream">Output stream</param>
  /// <param name="options">Options to use for the packed file</param>
  public static void WriteToPack(this IPlaneBase db, Stream stream, PlaneOptions options)
  {
    var salt = new byte[Constants.SALT_BYTES];
    RandomNumberGenerator.Fill(salt);
    using var table = new SSTableBuilder(stream, salt, options);
    var baseDB = db.BaseDB;
    foreach (var (key, value) in baseDB) {
      table.Put(key, value);
    }
  }
}

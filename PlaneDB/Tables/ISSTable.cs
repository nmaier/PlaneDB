using System;

namespace NMaier.PlaneDB;

internal interface ISSTable : IReadableTable, IDisposable
{
  long BloomBits { get; }
  long DiskSize { get; }
  long IndexBlockCount { get; }
  long RealSize { get; }
  void AddRef();
  void DeleteOnClose();
  void EnsureLazyInit();
}

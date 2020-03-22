using System;

namespace NMaier.PlaneDB
{
  internal interface IJournal : IWriteOnlyTable, IDisposable
  {
    long Length { get; }
  }
}
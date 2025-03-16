using System;

namespace NMaier.PlaneDB;

internal interface IJournal : IWritableTable, IDisposable
{
  long JournalLength { get; }
}

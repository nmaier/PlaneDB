namespace NMaier.PlaneDB;

internal interface IReadWriteTable : IReadableTable, IWritableTable
{
  long ApproxSize { get; }
  bool IsEmpty { get; }
}

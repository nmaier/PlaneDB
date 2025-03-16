namespace NMaier.PlaneDB;

internal interface IMemoryTable : IReadWriteTable
{
  long Generation { get; }
  IMemoryTable Clone();
}

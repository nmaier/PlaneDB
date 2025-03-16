namespace NMaier.PlaneDB;

internal enum CommandCode
{
  Fail = 0x1,
  Count,
  ReadOnly,
  AddOrUpdate,
  Clear,
  ContainsKey,
  GetOrAdd,
  Remove,
  Set,
  TryAdd,
  TryAdd2,
  TryGetValue,
  TryRemove,
  TryUpdate,
  Enumerate,
  EnumerateKeys,
  TableSpace,
  Flush
}

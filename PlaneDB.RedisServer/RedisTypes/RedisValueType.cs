namespace NMaier.PlaneDB.RedisTypes;

internal enum RedisValueType : byte
{
  String = 0,
  Integer = 1,
  Null = 2,
  List = 3,
  ListNode = 4,
  Set = 5,
  SetNode = 6
}

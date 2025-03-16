namespace NMaier.PlaneDB.RedisTypes;

internal enum RedisKeyType : byte
{
  Normal = 0,
  ListNode = 1,
  SetNode = 2,
  SetSentinel = 3
}

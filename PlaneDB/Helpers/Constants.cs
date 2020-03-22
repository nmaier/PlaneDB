namespace NMaier.PlaneDB
{
  internal static class Constants
  {
    internal const int MAGIC = 826426448; // "PDB1"
    internal const int TOMBSTONE = -1;
    internal static readonly byte[] MagicBytes = { 0x50, 0x42, 0x44, 0x31 };
  }
}
using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal static class Constants
{
  internal const MethodImplOptions HOT_METHOD = MethodImplOptions.AggressiveOptimization;
  internal const int INLINED_SIZE = 9;
  internal const long LEVEL_SMALL_TAIL_SIZE = 524288;
  internal const long LEVEL10_TARGET_SIZE = 2097152;
  internal const int MAGIC = 826426448; // "PDB1"
  internal const int MAX_ENTRIES_PER_INDEX_BLOCK = 128;
  internal const int MIN_ENTRIES_PER_INDEX_BLOCK = 8;
  internal const int SALT_BYTES = 32;
  internal const MethodImplOptions SHORT_METHOD = MethodImplOptions.AggressiveInlining;
  internal const int TOMBSTONE = -1;
  internal static readonly byte[] MagicBytes = "PBD1"u8.ToArray();
}

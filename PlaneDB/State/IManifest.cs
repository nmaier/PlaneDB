using System;
using System.Collections.Generic;
using System.IO;

namespace NMaier.PlaneDB;

internal interface IManifest : IDisposable
{
  byte[] Salt { get; }
  void AddToLevel(byte[] name, byte level, ulong id);
  ulong AllocateIdentifier();
  void ClearManifest();
  void CommitLevel(byte[] name, byte level, params ulong[] items);
  FileInfo FindFile(ulong id);
  void FlushManifest();
  SortedList<byte, ulong[]> GetAllLevels(byte[] name);
  byte GetHighestLevel(byte[] name);
  IEnumerable<ulong> Sequence(byte[] name);
  bool TryGetLevelIds(byte[] name, byte level, out ulong[] ids);
}

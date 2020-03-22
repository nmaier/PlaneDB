using System.IO;

namespace NMaier.PlaneDB.Tests
{
  internal sealed class KeepOpenMemoryStream : MemoryStream
  {
    public override void Close()
    {
    }

    protected override void Dispose(bool disposing)
    {
    }
  }
}
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace NMaier.PlaneDB.Tests;

internal sealed class KeepOpenMemoryStream : MemoryStream
{
  public override void Close()
  {
  }

  [SuppressMessage(
    "Usage",
    "CA2215:Dispose methods should call base class dispose",
    Justification = "As intended")]
  protected override void Dispose(bool disposing)
  {
  }
}

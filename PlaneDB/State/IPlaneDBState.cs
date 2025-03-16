namespace NMaier.PlaneDB;

internal interface IPlaneDBState : IManifest, IJournal
{
  IPlaneReadWriteLock ReadWriteLock { get; }
  void ClearJournal();
  void MaybeCompactManifest();
}

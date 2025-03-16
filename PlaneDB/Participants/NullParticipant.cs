using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class
  NullParticipant<TKey, TValue> : IPlaneDBMergeParticipant<TKey, TValue>
{
  public bool Equals(IPlaneDBMergeParticipant<TKey, TValue>? other)
  {
    return ReferenceEquals(this, other);
  }

  [MethodImpl(Constants.SHORT_METHOD)]
  public bool IsDataStale(in TKey key, in TValue value)
  {
    return false;
  }
}

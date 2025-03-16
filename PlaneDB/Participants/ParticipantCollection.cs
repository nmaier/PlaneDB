using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace NMaier.PlaneDB;

internal sealed class
  ParticipantCollection<TKey, TValue> : IPlaneDBMergeParticipant<TKey, TValue>
{
  private readonly int count;
  private readonly IPlaneDBMergeParticipant<TKey, TValue>[] participants;

  internal ParticipantCollection(
    IEnumerable<IPlaneDBMergeParticipant<TKey, TValue>> participants)
  {
    this.participants = participants.ToArray();
    count = this.participants.Length;
  }

  public bool Equals(IPlaneDBMergeParticipant<TKey, TValue>? other)
  {
    return ReferenceEquals(this, other);
  }

  [MethodImpl(Constants.HOT_METHOD)]
  public bool IsDataStale(in TKey key, in TValue value)
  {
    for (var i = 0; i < count; i++) {
      if (participants[i].IsDataStale(key, value)) {
        return true;
      }
    }

    return false;
  }
}

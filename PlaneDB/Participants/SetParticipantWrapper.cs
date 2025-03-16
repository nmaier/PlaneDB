namespace NMaier.PlaneDB;

internal sealed class
  SetParticipantWrapper<TKey> : IPlaneDBMergeParticipant<byte[], byte[]>
{
  private readonly IPlaneSerializer<TKey> serializer;
  private readonly IPlaneSetMergeParticipant<TKey> wrapped;

  internal SetParticipantWrapper(
    IPlaneSerializer<TKey> serializer,
    IPlaneSetMergeParticipant<TKey> wrapped)
  {
    this.serializer = serializer;
    this.wrapped = wrapped;
  }

  public bool Equals(IPlaneDBMergeParticipant<byte[], byte[]>? other)
  {
    return ReferenceEquals(this, other);
  }

  public bool IsDataStale(in byte[] key, in byte[] value)
  {
    return wrapped.IsDataStale(serializer.Deserialize(key));
  }
}

namespace NMaier.PlaneDB;

internal sealed class
  ParticipantWrapper<TKey, TValue> : IPlaneDBMergeParticipant<byte[], byte[]>
{
  private readonly IPlaneSerializer<TKey> keySerializer;
  private readonly IPlaneSerializer<TValue> valueSerializer;
  private readonly IPlaneDBMergeParticipant<TKey, TValue> wrapped;

  internal ParticipantWrapper(
    IPlaneSerializer<TKey> keySerializer,
    IPlaneSerializer<TValue> valueSerializer,
    IPlaneDBMergeParticipant<TKey, TValue> wrapped)
  {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.wrapped = wrapped;
  }

  public bool Equals(IPlaneDBMergeParticipant<byte[], byte[]>? other)
  {
    return ReferenceEquals(this, other);
  }

  public bool IsDataStale(in byte[] key, in byte[] value)
  {
    return wrapped.IsDataStale(
      keySerializer.Deserialize(key),
      valueSerializer.Deserialize(value));
  }
}

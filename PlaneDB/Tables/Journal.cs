using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NMaier.BlockStream;

namespace NMaier.PlaneDB;

internal sealed class Journal : IJournal
{
  internal static void ReplayOnto(
    Stream journalStream,
    ReadOnlySpan<byte> salt,
    PlaneOptions options,
    IWritableTable table)
  {
    var transformer = options.GetTransformerFor(salt);
    var actions = 0;
    _ = journalStream.Seek(0, SeekOrigin.Begin);
    try {
      if (journalStream.ReadInt32() != Constants.MAGIC) {
        throw journalStream switch {
          FileStream fs => new PlaneDBBrokenJournalException(fs),
          _ => new PlaneDBBrokenJournalException()
        };
      }
    }
    catch (EndOfStreamException) {
      throw journalStream switch {
        FileStream fs => new PlaneDBBrokenJournalException(fs),
        _ => new PlaneDBBrokenJournalException()
      };
    }

    using var journal = new SequentialBlockReadOnlyStream(journalStream, transformer);
    Span<byte> small = stackalloc byte[4096];

    for (;;) {
      try {
        var length = journal.ReadInt32() - sizeof(int);
        if (length < 5) {
          throw new IOException("Bad record");
        }

        var buffer = length <= 4096 ? small[..length] : new byte[length];
        journal.ReadFullBlock(buffer);

        var type = buffer[0];
        buffer = buffer[1..];

        switch ((RecordType)type) {
          case RecordType.Put: {
            if (length < 9) {
              throw new IOException("Bad record");
            }

            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(buffer);
            var valLen = BinaryPrimitives.ReadInt32LittleEndian(buffer[sizeof(int)..]);
            if (keyLen > length - 9 ||
                valLen > length - 9 ||
                keyLen + valLen > length - 9 ||
                keyLen <= 0 ||
                valLen < 0) {
              throw new IOException("Bad record");
            }

            var key = buffer.Slice(sizeof(int) * 2, keyLen);
            var val = buffer.Slice((sizeof(int) * 2) + keyLen, valLen);
            table.Put(key, val);

            break;
          }
          case RecordType.Remove: {
            var keyLen = BinaryPrimitives.ReadInt32LittleEndian(buffer);
            if (keyLen > length - 5 || keyLen <= 0) {
              throw new IOException("Bad record");
            }

            var key = buffer.Slice(sizeof(int), keyLen);
            table.Remove(key);

            break;
          }
          default:
            throw new IOException("Bad record");
        }

        ++actions;
      }
      catch (IOException) {
        break;
      }
    }

    if (actions > 0) {
      return;
    }

    throw journalStream switch {
      FileStream fs => new PlaneDBBrokenJournalException(fs),
      _ => new PlaneDBBrokenJournalException()
    };
  }

  private readonly CancellationTokenSource cancel = new();
  private readonly Task? flusher;
  private readonly bool fullySync;
  private readonly int maxActions;
  private readonly IPlaneReadWriteLock rwLock;
  private readonly SequentialBlockWriteOnceStream wrapped;
  private int actions;

  internal Journal(
    Stream stream,
    ReadOnlySpan<byte> salt,
    PlaneOptions options,
    IPlaneReadWriteLock rwLock)
  {
    this.rwLock = rwLock;
    var transformer = options.GetTransformerFor(salt);
    fullySync = options.MaxJournalActions < 0;
    maxActions = Math.Max(0, options.MaxJournalActions);
    stream.WriteInt32(Constants.MAGIC);
    wrapped = new SequentialBlockWriteOnceStream(stream, transformer);
    if (options.ThreadSafe) {
      flusher = Task.Factory.StartNew(
        RunFlushLoop,
        cancel.Token,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default);
    }
  }

  public void Dispose()
  {
    cancel.Cancel();
    flusher?.Wait();
    Flush(true);

    wrapped.Dispose();
    flusher?.Dispose();
    cancel.Dispose();
  }

  public long JournalLength => wrapped.Position;

  public void Flush()
  {
    Flush(true);
  }

  public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
  {
    WriteType(RecordType.Put, key, value);
  }

  public void Remove(ReadOnlySpan<byte> key)
  {
    var len = 1 + (sizeof(int) * 2) + key.Length;
    var buffer = ArrayPool<byte>.Shared.Rent(len);
    try {
      var span = buffer.AsSpan(0, len);
      BinaryPrimitives.WriteInt32LittleEndian(span, len);
      span[sizeof(int)] = (byte)RecordType.Remove;
      BinaryPrimitives.WriteInt32LittleEndian(span[(1 + sizeof(int))..], key.Length);
      key.CopyTo(span[(1 + (sizeof(int) * 2))..]);
      wrapped.Write(span);
      MaybeFlush();
    }
    finally {
      ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  private void Flush(bool force)
  {
    wrapped.Flush(force || fullySync);
    _ = Interlocked.Exchange(ref actions, 0);
  }

  private void MaybeFlush()
  {
    if (Interlocked.Increment(ref actions) < maxActions) {
      return;
    }

    Flush(true);
  }

  private async Task RunFlushLoop()
  {
    while (!cancel.IsCancellationRequested) {
      try {
        await Task.Delay(TimeSpan.FromSeconds(2), cancel.Token).ConfigureAwait(false);
      }
      catch {
        continue;
      }

      using (rwLock.AcquireWriteLock()) {
        var force = Interlocked.CompareExchange(ref actions, 0, 0) > 0;
        Flush(force);
        if (force) {
          _ = Interlocked.Exchange(ref actions, 0);
        }
      }
    }
  }

  [MethodImpl(Constants.HOT_METHOD | Constants.SHORT_METHOD)]
  private void WriteType(
    RecordType type,
    ReadOnlySpan<byte> key,
    ReadOnlySpan<byte> value)
  {
    var len = 1 + (sizeof(int) * 3) + key.Length + value.Length;
    var buffer = ArrayPool<byte>.Shared.Rent(len);
    try {
      var span = buffer.AsSpan(0, len);
      BinaryPrimitives.WriteInt32LittleEndian(span, len);
      span[sizeof(int)] = (byte)type;
      BinaryPrimitives.WriteInt32LittleEndian(span[(1 + sizeof(int))..], key.Length);
      BinaryPrimitives.WriteInt32LittleEndian(
        span[(1 + (sizeof(int) * 2))..],
        value.Length);
      key.CopyTo(span[(1 + (sizeof(int) * 3))..]);
      value.CopyTo(span[(1 + (sizeof(int) * 3) + key.Length)..]);
      wrapped.Write(span);
      MaybeFlush();
    }
    finally {
      ArrayPool<byte>.Shared.Return(buffer);
    }
  }

  private enum RecordType : byte
  {
    Put = 0,
    Remove = 1
  }
}

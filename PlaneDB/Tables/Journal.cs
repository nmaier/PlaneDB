using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NMaier.BlockStream;

namespace NMaier.PlaneDB
{
  internal sealed class Journal : IJournal
  {
    internal static void ReplayOnto(Stream journal, PlaneDBOptions options, IWriteOnlyTable table)
    {
      var transformer = options.BlockTransformer;
      var actions = 0;
      journal.Seek(0, SeekOrigin.Begin);
      if (journal.ReadInt32() != Constants.MAGIC) {
        throw new IOException("Bad journal file (wrong blocktransformer?)");
      }

      Span<byte> small = stackalloc byte[4096];

      for (;;) {
        try {
          var unlength = journal.ReadInt32();
          var length = journal.ReadInt32();
          if (length <= 0 || unlength <= 0) {
            throw new IOException("Bad record");
          }

          Span<byte> input = length <= 4096 ? small.Slice(0, length) : new byte[length];
          Span<byte> buffer = unlength <= 4096 ? small.Slice(0, unlength) : new byte[unlength];
          journal.ReadFullBlock(input);
          var tlen = transformer.UntransformBlock(input, buffer);
          if (tlen <= 0) {
            throw new IOException($"Bad record ({length}/{tlen})");
          }

          buffer = buffer.Slice(0, tlen);

          var type = buffer[0];
          buffer = buffer.Slice(1);

          switch ((RecordType)type) {
            case RecordType.Put: {
              var klen = BinaryPrimitives.ReadInt32LittleEndian(buffer);
              var vlen = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(sizeof(int)));
              var key = buffer.Slice(sizeof(int) * 2, klen);
              var val = buffer.Slice(sizeof(int) * 2 + klen, vlen);
              table.Put(key, val);
              ++actions;
              break;
            }
            case RecordType.Remove: {
              var klen = BinaryPrimitives.ReadInt32LittleEndian(buffer);
              var key = buffer.Slice(sizeof(int), klen);
              table.Remove(key);
              ++actions;
              break;
            }
            case RecordType.Update: {
              var klen = BinaryPrimitives.ReadInt32LittleEndian(buffer);
              var vlen = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(sizeof(int)));
              var key = buffer.Slice(sizeof(int) * 2, klen);
              var val = buffer.Slice(sizeof(int) * 2 + klen, vlen);
              table.Update(key, val);
              ++actions;
              break;
            }
            default:
              throw new ArgumentOutOfRangeException();
          }
        }
        catch (IOException) {
          break;
        }
      }

      if (actions > 0) {
        return;
      }

      throw journal switch {
        FileStream fs => new BrokenJournalException(fs),
        _ => new BrokenJournalException()
      };
    }

    private readonly CancellationTokenSource cancel = new CancellationTokenSource();
    private readonly Task flusher;
    private readonly int maxActions;
    private readonly Stream stream;
    private readonly IBlockTransformer transformer;
    private int actions;
    private readonly bool fullySync;

    internal Journal(Stream stream, PlaneDBOptions options)
    {
      this.stream = stream;
      transformer = options.BlockTransformer;
      fullySync = options.MaxJournalActions < 0;
      maxActions = Math.Max(0, options.MaxJournalActions);
      stream.WriteInt32(Constants.MAGIC);
      flusher = Task.Factory.StartNew(RunFlushLoop, cancel.Token, TaskCreationOptions.LongRunning,
                                      TaskScheduler.Current);
    }

    public void Dispose()
    {
      cancel.Cancel();
      flusher.Wait();
      Flush();

      stream.Dispose();
      flusher.Dispose();
      cancel.Dispose();
    }

    public long Length => stream.Length;

    public void Flush()
    {
      switch (stream) {
        case FileStream fs:
          fs.Flush(fullySync);
          break;
        default:
          stream.Flush();
          break;
      }

      Interlocked.Exchange(ref actions, 0);
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      WriteType(RecordType.Put, key, value);
    }

    public bool Remove(ReadOnlySpan<byte> key)
    {
      var len = 1 + sizeof(int) + key.Length;
      var buffer = ArrayPool<byte>.Shared.Rent(len);
      try {
        var span = buffer.AsSpan(0, len);
        span[0] = (byte)RecordType.Remove;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1), key.Length);
        key.CopyTo(span.Slice(1 + sizeof(int)));
        var transformed = transformer.TransformBlock(span);
        stream.WriteInt32((int)(Math.Ceiling(span.Length / 1024.0) * 1024.0));
        stream.WriteInt32(transformed.Length);
        stream.Write(transformed);
        MaybeFlush();
      }
      finally {
        ArrayPool<byte>.Shared.Return(buffer);
      }

      return true;
    }

    public bool Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      WriteType(RecordType.Update, key, value);
      return true;
    }

    private void MaybeFlush()
    {
      if (Interlocked.Increment(ref actions) < maxActions) {
        return;
      }

      Flush();
    }

    private async Task RunFlushLoop()
    {
      while (!cancel.IsCancellationRequested) {
        try {
          await Task.Delay(TimeSpan.FromSeconds(2), cancel.Token);
        }
        catch {
          continue;
        }

        Flush();
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void WriteType(RecordType type, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
      var len = 1 + sizeof(int) * 2 + key.Length + value.Length;
      var buffer = ArrayPool<byte>.Shared.Rent(len);
      try {
        var span = buffer.AsSpan(0, len);
        span[0] = (byte)type;
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1), key.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1 + sizeof(int)), value.Length);
        key.CopyTo(span.Slice(1 + sizeof(int) * 2));
        value.CopyTo(span.Slice(1 + sizeof(int) * 2 + key.Length));
        var transformed = transformer.TransformBlock(span);
        stream.WriteInt32((int)(Math.Ceiling(span.Length / 1024.0) * 1024.0));
        stream.WriteInt32(transformed.Length);
        stream.Write(transformed);
        MaybeFlush();
      }
      finally {
        ArrayPool<byte>.Shared.Return(buffer);
      }
    }

    private enum RecordType : byte
    {
      Put = 0,
      Remove = 1,
      Update = 2
    }
  }
}
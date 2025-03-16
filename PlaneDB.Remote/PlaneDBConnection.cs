using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NMaier.PlaneDB;

internal sealed class PlaneDBConnection(IPlaneDB<byte[], byte[]> db)
{
  private async Task HandleAddOrUpdate(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");

    _ = db.AddOrUpdate(key, AddValueFactory, UpdateValueFactory);
    await stream.WriteBool(true, token).ConfigureAwait(false);

    return;

    byte[] AddValueFactory()
    {
      return Task.Run(
          async () => {
            await stream.WriteBool(true, token).ConfigureAwait(false);
            await stream.FlushAsync(token).ConfigureAwait(false);

            return await stream.ReadArray(token).ConfigureAwait(false) ??
                   throw new IOException("Missing value");
          },
          token)
        .GetAwaiter()
        .GetResult();
    }

    byte[] UpdateValueFactory(in byte[] bytes)
    {
      var copy = bytes;

      return Task.Run(
          async () => {
            await stream.WriteBool(false, token).ConfigureAwait(false);
            await stream.WriteArray(copy, token).ConfigureAwait(false);
            await stream.FlushAsync(token).ConfigureAwait(false);

            return await stream.ReadArray(token).ConfigureAwait(false) ??
                   throw new IOException("Missing value");
          },
          token)
        .GetAwaiter()
        .GetResult();
    }
  }

  private async Task HandleCommand(Stream stream, CancellationToken token)
  {
    var command = await stream.ReadInt(token).ConfigureAwait(false);
    switch ((CommandCode)command) {
      case CommandCode.Fail:
        throw new NotSupportedException("This is a deliberate failure");
      case CommandCode.Count:
        await stream.WriteInt(db.Count, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);

        return;
      case CommandCode.ReadOnly:
        await stream.WriteBool(db.IsReadOnly, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);

        return;
      case CommandCode.AddOrUpdate:
        await HandleAddOrUpdate(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.Clear:
        db.Clear();
        await stream.WriteBool(true, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);

        return;
      case CommandCode.ContainsKey:
        await HandleContainsKey(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.GetOrAdd:
        await HandleGetOrAdd(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.Remove:
        await HandleRemove(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.Set:
        await HandleSet(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TryAdd:
        await HandleTryAdd(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TryAdd2:
        await HandleTryAdd2(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TryGetValue:
        await HandleTryGetValue(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TryRemove:
        await HandleTryRemove(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TryUpdate:
        await HandleTryUpdate(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.Enumerate:
        await HandleEnumerate(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.EnumerateKeys:
        await HandleEnumerateKeys(stream, token).ConfigureAwait(false);

        return;
      case CommandCode.TableSpace:
        await stream.WriteArray(Encoding.UTF8.GetBytes(db.TableSpace), token)
          .ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);

        return;
      case CommandCode.Flush:
        db.Flush();
        await stream.WriteBool(true, token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);

        return;
      default:
        throw new IOException("Invalid command");
    }
  }

  private async Task HandleContainsKey(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    await stream.WriteBool(db.ContainsKey(key), token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleEnumerate(Stream stream, CancellationToken token)
  {
    await foreach (var (key, value) in db.WithCancellation(token).ConfigureAwait(false)) {
      await stream.WriteArray(key, token).ConfigureAwait(false);
      await stream.WriteArray(value, token).ConfigureAwait(false);
    }

    await stream.WriteMissing(token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleEnumerateKeys(Stream stream, CancellationToken token)
  {
    await foreach (var key in db.GetKeysIteratorAsync(token).ConfigureAwait(false)) {
      await stream.WriteArray(key, token).ConfigureAwait(false);
    }

    await stream.WriteMissing(token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleGetOrAdd(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    var added = false;
    var val = db.GetOrAdd(
      key,
      () => Task.Run(
          async () => {
            added = true;
            await stream.WriteMissing(token).ConfigureAwait(false);
            await stream.FlushAsync(token).ConfigureAwait(false);
            var rv = await stream.ReadArray(token).ConfigureAwait(false) ??
                     throw new IOException("Missing value");
            await stream.WriteBool(added, token).ConfigureAwait(false);
            await stream.FlushAsync(token).ConfigureAwait(false);

            return rv;
          },
          token)
        .GetAwaiter()
        .GetResult());
    if (!added) {
      await stream.WriteArray(val, token).ConfigureAwait(false);
      await stream.FlushAsync(token).ConfigureAwait(false);
    }
  }

  private async Task HandleRemove(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    await stream.WriteBool(db.Remove(key), token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleSet(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    var value = await stream.ReadArray(token).ConfigureAwait(false) ??
                throw new IOException("Missing value");
    db.SetValue(key, value);
    await stream.WriteBool(true, token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleTryAdd(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    var value = await stream.ReadArray(token).ConfigureAwait(false) ??
                throw new IOException("Missing value");
    await stream.WriteBool(db.TryAdd(key, value), token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleTryAdd2(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    var value = await stream.ReadArray(token).ConfigureAwait(false) ??
                throw new IOException("Missing value");
    if (db.TryAdd(key, value, out var existing)) {
      await stream.WriteMissing(token).ConfigureAwait(false);
    }
    else {
      await stream.WriteArray(existing, token).ConfigureAwait(false);
    }

    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleTryGetValue(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    if (!db.TryGetValue(key, out var value)) {
      await stream.WriteMissing(token).ConfigureAwait(false);
    }
    else {
      await stream.WriteArray(value, token).ConfigureAwait(false);
    }

    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleTryRemove(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false);
    if (!db.TryRemove(key ?? throw new IOException("Missing key"), out var value)) {
      await stream.WriteMissing(token).ConfigureAwait(false);

      return;
    }

    await stream.WriteArray(value, token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  private async Task HandleTryUpdate(Stream stream, CancellationToken token)
  {
    var key = await stream.ReadArray(token).ConfigureAwait(false) ??
              throw new IOException("Missing key");
    var value = await stream.ReadArray(token).ConfigureAwait(false) ??
                throw new IOException("Missing value");
    var comp = await stream.ReadArray(token).ConfigureAwait(false) ??
               throw new IOException("Missing comparison value");
    await stream.WriteBool(db.TryUpdate(key, value, comp), token).ConfigureAwait(false);
    await stream.FlushAsync(token).ConfigureAwait(false);
  }

  internal async Task Process(Stream stream, CancellationToken token)
  {
    var seedBytes = new byte[PlaneDBRemoteOptions.NONCE_LENGTH];
    PlaneDBRemoteOptions.FillNonce(seedBytes);
    var seed = BinaryPrimitives.ReadInt32BigEndian(seedBytes);
    await stream.WriteInt(seed, token).ConfigureAwait(false);
    var rnd = new PlaneProtocolRandom(seed);

    while (!token.IsCancellationRequested) {
      var canary = await stream.ReadInt(token).ConfigureAwait(false);
      if (canary != rnd.Next()) {
        throw new IOException("Protocol canary mismatch");
      }

      await HandleCommand(stream, token).ConfigureAwait(false);
    }
  }
}

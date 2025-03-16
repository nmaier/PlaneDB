using System;
using System.Collections.Concurrent;
using System.Threading;

namespace NMaier.PlaneDB;

internal sealed class BackgroundActionQueue : IDisposable
{
  private readonly BlockingCollection<Action> queue = new(new ConcurrentQueue<Action>());
  private readonly Thread thread;

  internal BackgroundActionQueue()
  {
    thread = new Thread(Loop) {
      IsBackground = true,
      Name = "PlaneDB-BackgroundActions"
    };
    thread.Start();
  }

  public void Dispose()
  {
    queue.CompleteAdding();
    thread.Join();
  }

  private void Loop()
  {
    try {
      foreach (var action in queue.GetConsumingEnumerable()) {
        try {
          action();
        }
        catch {
          // ignored
        }
      }
    }
    catch {
      // ignored
    }
  }

  internal void Queue(Action action)
  {
    try {
      _ = queue.TryAdd(action);
    }
    catch {
      // ignored
    }
  }
}

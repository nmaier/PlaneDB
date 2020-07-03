using System;
using System.Collections.Concurrent;
using System.Threading;

namespace NMaier.PlaneDB
{
  internal sealed class BackgroundActionQueue : IDisposable
  {
    private readonly BlockingCollection<Action> queue = new BlockingCollection<Action>(new ConcurrentQueue<Action>());
    private readonly Thread thread;

    internal BackgroundActionQueue()
    {
      thread = new Thread(Loop) { IsBackground = true };
      thread.Start();
    }

    public void Dispose()
    {
      queue.CompleteAdding();
      thread.Join();
    }

    internal void Queue(Action action)
    {
      try {
        queue.TryAdd(action);
      }
      catch {
        // ignored
      }
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
  }
}
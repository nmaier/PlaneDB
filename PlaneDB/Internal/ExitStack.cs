using System;
using System.Collections.Generic;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

[PublicAPI]
internal sealed class ExitStack : IDisposable
{
  private readonly HashSet<IDisposable> tracked = [];

  public void Dispose()
  {
    List<Exception>? exceptions = null;
    lock (this) {
      foreach (var disposable in tracked) {
        try {
          disposable.Dispose();
        }
        catch (Exception ex) {
          exceptions ??= [];
          exceptions.Add(ex);
        }
      }

      tracked.Clear();
    }

    if (exceptions?.Count > 0) {
      throw new AggregateException(exceptions);
    }
  }

  public T Register<T>(T obj) where T : IDisposable
  {
    lock (this) {
      _ = tracked.Add(obj);
    }

    return obj;
  }
}

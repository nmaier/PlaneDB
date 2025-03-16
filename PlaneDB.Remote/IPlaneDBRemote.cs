using System;
using System.Net;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace NMaier.PlaneDB;

/// <summary>
///   Information about a (running) PlaneDB server
/// </summary>
[PublicAPI]
public interface IPlaneDBRemote : IDisposable
{
  /// <summary>
  ///   Listen address
  /// </summary>
  IPAddress Address { get; }

  /// <summary>
  ///   Final listen port
  /// </summary>
  int Port { get; }

  /// <summary>
  ///   Awaitable task
  /// </summary>
  Task? ServerTask { get; }

  /// <summary>
  ///   Stops the server
  /// </summary>
  void StopServer();

  /// <summary>
  ///   Wait for the server to stop listening
  /// </summary>
  void Wait();
}

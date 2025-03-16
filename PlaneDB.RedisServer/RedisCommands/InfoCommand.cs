using System;
using System.Text;

using NMaier.PlaneDB.RedisProtocol;
#if NET6_0_OR_GREATER
using System.Globalization;
#endif

namespace NMaier.PlaneDB.RedisCommands;

internal sealed class InfoCommand : IRedisCommand
{
  private static void AddClients(StringBuilder sb)
  {
    _ = sb.AppendLine("# Clients");
    _ = sb.AppendLine("maxclients:500");
  }

  private static void AddServer(StringBuilder sb)
  {
    _ = sb.AppendLine("# Server");
    _ = sb.AppendLine("redis_version:6.1.0");
    _ = sb.AppendLine("redis_mode:standalone");
#if NET6_0_OR_GREATER
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"hz:{TimeSpan.TicksPerSecond}");
    _ = sb.AppendLine(
      CultureInfo.InvariantCulture,
      $"configured_hz:{TimeSpan.TicksPerSecond}");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"arch_bits:{IntPtr.Size * 8}");
    _ = sb.AppendLine(
      CultureInfo.InvariantCulture,
      $"server_time_in_usec:{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000}");
#else
    _ = sb.AppendLine($"hz:{TimeSpan.TicksPerSecond}");
    _ = sb.AppendLine($"configured_hz:{TimeSpan.TicksPerSecond}");
    _ = sb.AppendLine($"arch_bits:{IntPtr.Size * 8}");
    _ = sb.AppendLine(
      $"server_time_in_usec:{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000}");
#endif
  }

  public RespType Execute(RedisServerClient client, string cmd, RespType[] args)
  {
    var sb = new StringBuilder();

    if (args.Length == 0) {
      AddServer(sb);
      AddClients(sb);
    }
    else {
      switch (args[0].AsString().ToLowerInvariant()) {
        case "server":
          AddServer(sb);

          break;
        case "clients":
          AddClients(sb);

          break;
      }
    }

    return new RespBulkString(sb.ToString());
  }

  public int MaxArgs => 1;
  public int MinArgs => 0;
}

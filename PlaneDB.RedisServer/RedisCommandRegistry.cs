using System;
using System.Collections.Generic;

using NMaier.PlaneDB.RedisCommands;

namespace NMaier.PlaneDB;

internal sealed class RedisCommandRegistry : Dictionary<string, IRedisCommand>
{
  public RedisCommandRegistry() : base(StringComparer.OrdinalIgnoreCase)
  {
    Add("info", new InfoCommand());
    Add("time", new TimeCommand());

    Add("save", new NoOpCommand());
    Add("bgsave", new NoOpCommand());
    Add("bgrewriteaof", new NoOpCommand());
    Add("dbsize", new DBSizeCommand());
    Add("lastsave", new LastSaveCommand());
    Add("flushall", new FlushCommand());
    Add("flushdb", new FlushCommand());

    Add("echo", new EchoCommand());
    Add("ping", new PingCommand());

    Add("get", new GetCommand());
    Add("getrange", new GetRangeCommand());
    Add("keys", new KeysCommand());
    Add("del", new DelCommand());
    Add("unlink", new DelCommand());
    Add("getset", new GetSetCommand());
    Add("exists", new ExistsCommand());
    Add("set", new SetCommand());
    Add("setNX", new SetNXCommand());
    Add("setex", new SetEXCommand());
    Add("psetex", new SetEXCommand());
    Add("setrange", new SetRangeCommand());
    Add("append", new AppendCommand());
    Add("rename", new RenameCommand());
    Add("renamenx", new RenameCommand());
    Add("strlen", new StrLenCommand());

    Add("getbit", new GetBitCommand());
    Add("setbit", new SetBitCommand());
    Add("bitcount", new BitCountCommand());

    Add("incr", new IncDecrCommand());
    Add("decr", new IncDecrCommand());
    Add("incrby", new IncDecrByCommand());
    Add("decrby", new IncDecrByCommand());

    Add("mget", new MGetCommand());
    Add("mset", new MSetCommand());
    Add("msetnx", new MSetNXCommand());

    Add("ttl", new TTLCommand());
    Add("pttl", new TTLCommand());
    Add("persist", new ExpireCommand());
    Add("expire", new ExpireCommand());
    Add("expireat", new ExpireCommand());
    Add("pexpire", new ExpireCommand());
    Add("pexpireat", new ExpireCommand());

    Add("llen", new ListLenCommand());
    Add("lindex", new ListIndexCommand());
    Add("lset", new ListSetCommand());
    Add("lpos", new ListPosCommand());
    Add("lpush", new ListPushCommand());
    Add("lpushx", new ListPushCommand());
    Add("rpush", new ListPushCommand());
    Add("rpushx", new ListPushCommand());
    Add("lpop", new ListPopCommand());
    Add("rpop", new ListPopCommand());
    Add("lrange", new ListRangeCommand());

    Add("scard", new SetCardinaltyCommand());
    Add("sismember", new SetIsMemberCommand());
    Add("smismember", new SetIsMemberCommand());
    Add("smembers", new SetMembersCommand());
    Add("sadd", new SetAddCommand());
    Add("srem", new SetRemoveCommand());
    Add("spop", new SetPopCommand());
    Add("srandmember", new SetPopCommand());
  }
}

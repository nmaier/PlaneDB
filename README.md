# ![Icon](icon.png) PlaneDB

Kinda like LevelDB, but in C#

![.NET Core](https://github.com/nmaier/PlaneDB/workflows/.NET%20Core/badge.svg)

## What?
PlaneDB is a key-value store or database written in pure C#. It uses a log structured merge tree approach, with a memory database with journal for recently changed keys, sorted string tables (sstable) in multiple levels for on disk storage, and a block based approach, allowing features such as on-disk data compression or encryption.

Aside from the key-value PlaneDB implementations (`PlaneDB`, `TypedPlaneDB<TKey, TValue>`, `StringPlaneDB`), there are PlaneSet implementations (`PlaneSet`, `TypedPlaneSet<T>`, `StringPlaneSet`) using the same underlying architecture, but optimized for storing sets.

## Features

- General byte-array based key-value store (`PlaneDB`), and set (`PlaneSet`)
- Typed key-value/set store using serializers (`TypedPlaneDB`, `TypedPlaneSet`) including a string stores (`StringPlaneDB`, `StringPlaneSet`)
- Configurable on-disk compression, encryption. Uses my `BlockStream` library, which comes with lz4 compression and a ChaCha20-Poly1305 cipher/mac implementation.
- Customizable byte-array comparer.
- Thread-safety (and Task-safety) by default.
- Crash resilience using read-only/write-once tables and a journal for the most recent changes. By default the journal is flushed either once every 2 seconds, or whenever a certain amount of write operations were performed to conserve disk writes, meaning the last few operations may be lost upon unclean shutdown, but this can be configured to flush upon each write `.MakeFullySync()/.FLushJournalAfterNumerOfWrite(...)`.
- Automatic bloom filters to avoid useless reads, using `xxHash` hashes.
- API interface implementing `IDictionary<>` and various `ConcurrentDictionary` methods.
- API interface implementing `ISet<>` and various additional methods.
- Multiple stores (table spaces) in a single directory.
- Data (sstable) files, once written, are read-only. Additional data is always stored in new files (after first collected in a memory table), which maybe be merged together into yet new files (higher levels).

## Example
```c#
var location = new DirectoryInfo(".");
var options = new PlaneDBOptions().EnableCompression().MakeFullySync();
using (var db = new StringPlaneDB(location, FileMode.CreateNew, options) {
  db["abc"] = "def"; // Direct set
  db.AddOrUpdate("abc", "ghi", (_, __) => "jkl"); // Will call the update function
  db.TryAdd("abc", "mno"); // Ignored
}

using (var db = new StringPlaneDB(location, FileMode.OpenOrCreate, options) {
  Console.WriteLine($"abc={db["abc"]}"); // abc=jkl
}
```

Word of advise: Do not excessively use `.Count`/`.Keys`/`.Values`, as these properties need to perform a full iteration over the entire data set.

## Performance
Hmmm... good enough? In my unscientific synthetic benchmarks the performance is comparable to that of LevelDB, with wallclock times between 0.4x - 2.0x compared to RocksDB(Sharp), depending on workload and configuration.

## Limitations

- No support for column families or such (yet)
- Keys and values have to fit into your memory. Not at the same time, but whenever they are read. While large keys values up to 2GB each are supported in theory, in practise you're bound by memory and also this has been untested.

## File Structure

- `LOCK` - To ensure exclusivity. Always empty.
- `MANIFEST` - Stores information about what sstable files belong to what level and the generation counter
- `JOURNAL` - Recovery information in case of unclean shutdowns.
- `###` - readonly sstable files storing the actual key and value data, as well as pre-computed bloom filters.
	
## Status
Beta-grade software.
I have been dogfooding the code for a while in various internal applications of mine (none business-critical), with millions of records, along with the test suite.

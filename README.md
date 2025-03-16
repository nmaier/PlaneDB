# ![Icon](https://github.com/nmaier/PlaneDB/raw/master/icon.png) PlaneDB

Kinda like LevelDB, but in C#

![.NET CI Status](https://github.com/nmaier/PlaneDB/workflows/.NET%20CI/badge.svg)
![Nuget](https://img.shields.io/nuget/v/NMaier.PlaneDB)
![GitHub](https://img.shields.io/github/license/nmaier/PlaneDB)

## Overview

PlaneDB is a key-value store or database written in pure C#. It uses a log structured merge tree approach, with a memory database plus journal for recently changed keys, sorted string tables (SST) in multiple levels for on disk storage, and a block based approach, allowing features such as on-disk data compression or encryption.

Aside from the key-value PlaneDB implementations (`PlaneDB`, `TypedPlaneDB<TKey, TValue>`, `StringPlaneDB`), there are PlaneSet implementations (`PlaneSet`, `TypedPlaneSet<T>`, `StringPlaneSet`) using the same underlying architecture, but optimized for storing sets.

## Features

- General byte-array based key-value store (`PlaneDB`), and set (`PlaneSet`)
- Typed key-value/set store using serializers (`TypedPlaneDB`, `TypedPlaneSet`) including string stores (`StringPlaneDB`, `StringPlaneSet`)
- Configurable on-disk compression, encryption. Uses my `BlockStream` library, which comes with lz4 compression and a ChaCha20-Poly1305 cipher/mac implementation.
- Customizable byte-array comparer.
- Thread-safety (and Task-safety) by default.
- Tunable crash resilience using read-only/write-once tables and a journal. By default the journal is flushed either once every 2 seconds, or whenever a certain amount of write operations were performed to conserve disk writes, meaning the last few operations may be lost upon unclean shutdown, but this can be configured to flush upon each write `.MakeFullySync()/.FLushJournalAfterNumberOfWrite(...)`. Journals can be entirely disabled.
- Automatic bloom filters to avoid useless reads, using `xxHash` hashes.
- API interface implementing `IDictionary<>` and various `ConcurrentDictionary` methods.
- API interface implementing `ISet<>` and various additional methods.
- Multiple stores (table spaces) in a single directory.
- Data (SST) files, once written, are read-only. Additional data is always stored in new files (after first collected into memory tables), which are automatically merged together into bigger SST files as needed (higher levels).

## Compatibility

At the moment PlaneDB is tested to work on

- `net5.0`
- `net48` (Framework) - performs a little worse, mostly due to necessary shims working around missing `Span<>` features in the runtime.

The primary development focus is on a well-performing library for `net5.0`.

## Example

```c#
using System;
using System.IO;

using NMaier.PlaneDB;

var location = new DirectoryInfo(".");
var baseOptions = new PlaneDBOptions().WithCompression();
var writeOptions = baseOptions.MakeFullySync();

// PlaneDBs are just key-value databases mapping byte-array keys
// to byte array values.
// But there are specialized variants which allow to use
// different types as well. The most common such specialized type
// is StringPlaneDB, which offers a string-to-string key-value db.
// It is functionally equivalent to:
//    TypedPlaneDB<string, string>(
//      new PlaneStringSerializer(),
//      new PlaneStringSerializer(),
//      ...
//    )
using (var db = new StringPlaneDB(location, writeOptions)) {
  // Direct set
  db["abc"] = "def";
  // Direct set, alternative syntax
  db.SetValue("abc", "def");
  // Will call the update function, as the key is already present
  db.AddOrUpdate("abc", "ghi", (_, _) => "jkl");
  // Add will not be successful, as key exists already
  db.TryAdd("abc", "mno"); // returns false

  Console.WriteLine($"abc={db["abc"]}");
  // abc=jkl, because the update function updated the key last
}

var readOptions = baseOptions.WithOpenMode(PlaneDBOpenMode.ReadOnly);
// Open the existing DB read-only
using (var db = new StringPlaneDB(location, readOptions)) {
  // abc=jkl, from above
  Console.WriteLine($"abc={db["abc"]}");

  try {
    // will throw, as the db was opened read-only
    db["abc"] = "def";
  }
  catch (PlaneDBReadOnlyException) {
    Console.Error.WriteLine("Read-only!");
  }
}

// Using typed PlaneDBs is easy too. All you need is to specify
// appropriate serializers.
// PlaneDB comes with serializers for common types, and there is
// also PlaneDB.MessagePack and PlaneDB.Json providing additional
// serializers for structured data.
// Or you can write your own, implementing the IPlaneSerializer<T>
// interface.
using (var db =
  new TypedPlaneDB<string, long>(new PlaneStringSerializer(), new PlaneInt64Serializer(), location, writeOptions)) {
  db["abc"] = 1; // Direct set
  db.SetValue("abc", 2); // Direct set, alternative syntax
  db.AddOrUpdate("abc", 3, (_, _) => 4); // Will call the update function
  db.TryAdd("abc", 5); // returns false

  Console.WriteLine($"abc={db["abc"]}");
  // abc=4, because the update function was called
}
```

## Performance

Hmmm... good enough? In my unscientific synthetic benchmarks the performance is comparable to that of LevelDB, with wall-clock times between 0.4x - 2.0x compared to RocksDB(Sharp), depending on workload and configuration.

Word of advise: Do not excessively use `.Count`/`.Keys`/`.Values`, as these properties need to perform a full iteration over the entire data set.

Also, avoid expensive serializers, in particular for keys.

## Limitations

- No support for column families or such (yet)
- Keys and values have to fit into your memory. Not all at the same time, but whenever they are read. While large keys and values up to 2GB each are supported in theory, in practice you're bound by memory. There is no streaming interface (yet)
- PlaneDB was designed for "short" keys and values, where short up to a few kilobytes for keys and up to a few megabytes for values. It will work with larger keys and values too, but it may perform less well in such cases.
- PlaneDB does not offer transactions, i.e. updating multiple keys or none. You can however avoid data races by e.g. using `AddOrUpdate(key, addFactory, updateFactory)`, relying on the fact that when the factory methods are called you the database is locked against other writes. Or you can use `MassRead`/`MassInsert`, which come with some performance penalty.

## Failure Modes

Unless you're using PlaneDB in a non-supported way, e.g. using key serializers that do not produce stable output for the same values, there are three important failure modes.

When the operating system suddenly shuts down, e.g. due to power loss or due to a kernel panic, then data may not be written to the disk, even if the writes were already scheduled with the operating system. To avoid losing data PlaneDB by default uses write-ahead journals, which are synced to disk by default after a certain amount of writes or time elapsed. This default was chosen as a balance between performance and data integrity. In this default mode a sudden shutdown will loose at most the last few writes.

These journals can be disabled, or made fully-sync meaning PlaneDB will wait for all journal writes to be flushed to disk (according to the operating system and underlying hardware) before continuing, which comes with a performance cost.

SST files should not be affected either as they are write-once and the read-only, and an SST will only be added to the manifest after it was written and synced to disk. If a sudden outage occurs during writing such an SST file, the file will not be in the manifest, and will be removed as an orphan automatically when the PlaneDB is next opened. The *lost* data will be still in the journal and reconstructed from there.

The underlying disc or file system may become out of order or read-only, e.g. when no more free space is available (or the user quota is exhausted), or upon hardware or network errors. When this happens, PlaneDB can of course not write any data, and will start raising exceptions to the application. The point of when this happens may vary depending on the journal mode, by default it should not happen any later than 2 seconds after the underlying error occurs, in fully sync mode it should occur immediately. With journals disabled it will only be noticed when a new SST file is written, which of course depends on the write rate.

Then there are "silent" disk failures, where individual bits or areas of the disc become corrupted. In normal modes (with/without encryption or compression) will use a on-disk format that will detect *some* of such bit errors:

- By default PlaneDB (or rather the underlying `BlockStream`s) use a CRC for data blocks. This is not a total protection against silent failures, but reduces the risk significantly.
- When using compression, the data integrity checks are handled by `lz4` and the `xhash-32` checksums it uses. This is not a total protection against silent failures, either.
- When using encryption, then additionally data is additionally verified with the Poly1305 MAC. This is close to, but not quite, a total protection against silent failure. The probability of silent failures that will by mere chance still generate a valid MAC result is *very* low.

## File Structure

A planedb storage location is a directory comprising multiple files. File names have the table space as their prefix ("default" if not specified in the options) and `.planedb` file extensions.

- `LOCK` - To ensure exclusivity. Always empty, and can be missing (will be created when a PlaneDB is opened).
- `MANIFEST` - Stores information about what (SST) files belong to what level and the generation counter.
- `JOURNAL` - Recovery information in case of unclean shutdowns. May be empty or missing when journals are disabled in the options.
- `###` - readonly SST files storing the actual key and value data, a main index and additional index blocks, as well as pre-computed bloom filters. If the database is empty, no such files will exist.

There is also a read-only packed variant (`PlaneDBOpenMode.Packed`) that will read a single SST file. The file name and location is entirely up to the application. Packs of an existing PlaneDB may be created with `IPlaneDB.WriteToPack()` or PlaneTool.

## Status

Beta-grade software.
I have been dogfooding the code for a while in various internal applications of mine (none business-critical), with millions of records, along with running the unit test suite a lot.

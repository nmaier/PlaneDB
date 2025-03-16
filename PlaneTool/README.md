# ![Icon](https://github.com/nmaier/PlaneDB/raw/master/icon.png) PlaneTool

The PlaneTool helper is a simple command line tool aimed to help you maintain and/or convert PlaneDBs

![.NET CI Status](https://github.com/nmaier/PlaneDB/workflows/.NET%20CI/badge.svg)
![Nuget](https://img.shields.io/nuget/v/NMaier.PlaneDB)
![GitHub](https://img.shields.io/github/license/nmaier/PlaneDB)

## Commands

For options to individual tools consult `PlaneTool help <command>`.

- `info` - Print various information and statistics about a PlaneDB.
- `compact` - Compact a PlaneDB. This will iterate over all the current data stored within the DB, recreate SST files in proper ordering, and finally do a merge compaction. It also offers a `--repair` mode to preserve as much data from a broken PlaneDB file set as possible, if possible.
- `dump` - Dumps all the data (key/values) into a flat file.
- `restore` Restore Import data from a previously dumped flat file.
- `importrocksdb` - Import a RocksDB(Sharp) and write out a PlaneDB with the data.
- `pack` - Packs an entire PlaneDB into a single SST pack file that can be opened read-only using `PlaneDBOpenMode.Packed`.
- `serve` - Serve a PlaneDB using the PlaneDB.Remote binary protocol implementation or the Redis protocol implementation.

## Dump file structure

A file magic of `PDBD` followed by zero or more records:

| Length | Purpose                    |
| ------ | -------------------------- |
| `0x4`  | Key Length `K` (`Int32`)   |
| `0x4`  | Value Length `V` (`Int32`) |
| `K`    | Key bytes                  |
| `V`    | Value bytes                |

Records will be ordered according to the default byte array comparer.

# Manifest file format

A manifest file is a regular file storing important metadata including the generation counter and sanity checks.

All numbers are little-endian.

## Header

| Length | Purpose                           |
| ------ | --------------------------------- |
| `0x4`  | File magic (`0x31424450`, "PDB1") |
| `0x8`  | generation counter (`Int64`) \[1] |
| `0x4`  | Block transformer magic length    |
| `var`  | Block transformer magic \[2]      |
| `...`  | Zero or more Level Records \[3]   |

1. The generation counter is a counter meant to point to the first available Data File Id. It should be only increasing. Data File Ids aren't necessarily used/have a corresponding data file (e.g. might have been removed during compaction merges, or over-allocated in the first place).
2. Little endian 32-bit representation of the file magic `0x31424450` put through the configured block transformer. Used to detect attempts to open a PlaneDB location with incompatible block transformer options. If the specified block transformer cannot decode the input block, or the resulting output block does not match the file magic bytes, implementations must stop processing the manifest file and inform the user of the (configuration) error.
3. Can be named or unnamed records.

## Level Records (common)

Level records can be named or unnamed (essentially using an empty name). Support for names will allow implementations to add features such as column families.

| Length | Purpose                                |
| ------ | -------------------------------------- |
| `0x1`  | Level number (unsigned `byte`)         |
| `0x4`  | Number `N` of level ids (`Int32`) \[1] |
| ...    |                                        |

1. If `N` is a positive number `>= 0` then the record is an unnamed record, otherwise a named record.

## Level Records (unnamed)

| Length    | Purpose                         |
| --------- | ------------------------------- |
| `0x1`     | Level number (unsigned `byte`)  |
| `0x4`     | Number N of level ids (`Int32`) |
| `0x8 * N` | Data File Id (`Int64`)          |

## Level Records (named)

| Length     | Purpose                              |
| ---------- | ------------------------------------ |
| `0x1`      | Level number (unsigned `byte`)       |
| `0x4`      | Number N of level ids (`Int32`) \[1] |
| `0x4`      | Length L of the name in bytes        |
| `L`        | Name bytes (if any) \[2]             |
| `0x8 * -N` | Data file ids (`Int64`)              |

1. If `N` is `Int32.MinValue`, then there are 0 data file ids, otherwise there are `-N` data file ids.
2. Names are UTF-8 by convention, although the file format can handle any name bytes.

## Processing

Each `(name bytes, level number)` tuple key is unique in that records later in the manifest file override previous records for the same tuple key. Implementations are encouraged not to overwrite record, but always append new records during normal operation, writing out clean new manifest files with outdated level records removed (compaction) as needed. New records thus can be written by seeking to the end of the file and writing out the record data (followed by a disk flush for durability).

The generation counter is at a fixed offset (`0x4`), thus allowing to easily seek to the position within the file without the need to calculate any offsets or keep any state about the file structure.

# Data file format

Data files are sorted string tables (SST) where strings are just arbitrary sequences of bytes.

All numbers are little-endian.

PlaneDB will normally store and read data files using the `NMaier.BlockStreams` stream wrapper library.

| Length | Purpose                 |
| ------ | ----------------------- |
| `...`  | Values block            |
| `...`  | Key blocks              |
| `...`  | Header block            |
| `0x8`  | Header offset (`Int64`) |

The way to read an SST file is to seek to the end, read the header offset, then the Header, construct the bloom filter and index.

Key and corresponding values are accessed by consulting the bloom filter then index to find the corresponding key block, and then reading the key block, and finally the value (either inlined or from the values block).

## Header Block

| Length    | Purpose                               |
| --------- | ------------------------------------- |
| `0x4`     | Bloom Filter Length `B` (`Int32`)     |
| `-B`      | Bloom filter (sequence of bits) \[1]  |
| `0x4`     | Number of Index entries `I` (`Int32`) |
| `... * I` | Index entries                         |

1. The bloom filter length is given as a negative number for compatibility reasons with a previous incarnation of the data format that isn't in use anymore.

### Index Entry

| Length | Purpose                        |
| ------ | ------------------------------ |
| `0x4`  | Logical offset (`Int32`) \[1]  |
| `0x4`  | Start Key Length `K` (`Int32`) |
| `K`    | Key bytes \[2]                 |

1. The logical offset from the header offset of the key block corresponding to this Index Entry
2. The writer implementation may attempt to find the shortest possible key bytes sequence that will uniquely identify a block. Therefore it is not necessarily true that the key byte sequence of an index entry refers to a key actually present within the data.

## Key Blocks

| Length    | Purpose                                     |
| --------- | ------------------------------------------- |
| `0x4`     | Number of Key Records `K` (`Int32`)         |
| `0x8`     | Base offset into the Values Block (`Int64`) |
| `... * K` | Key Records                                 |

### Key Records

| Length | Purpose                         |
| ------ | ------------------------------- |
| `0x4`  | Key length `K` (`Int32`)        |
| `0x4`  | Value length `V` (`Int32`) \[1] |
| `K`    | Key bytes                       |
| `V`    | Value bytes (if inlined) \[2]   |

1. Value length is basically an union type
   - A value of `-1` marks a tombstone, i.e. a removed key. There is no value
   - A value `< -1` marks an inlined value. The real length of the value will be `-(V >> 4)`. Inlined values are supposed to be less than 9 bytes in length (which will fit nullable `Int64` values). Inlining will reduce the number of required file seeks and reads.
   - Otherwise it's the length of the value verbatim, and the value is not inlined but found in the values block.
2. Inlined values are located directly within the key record. Non-inlined values can be accessed by calculating `key block base offset + sum(length of preceding non-inlined values)` to find the absolute offset of the value.

## Values Block

| Length | Purpose             |
| ------ | ------------------- |
| `...`  | Value byte sequence |

Values in the file are sorted by their corresponding key using the library or user specified byte array comparer. The lengths of individual values are found in the key blocks. Short values may be inlined in the key blocks and not appear in the values block at all.

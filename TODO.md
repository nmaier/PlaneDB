# TODO

- Test "disk full"/"disk error" scenarios
  - Dump memory table before inserting keys to stop it from accepting keys in such situations
  - Dump to journal before dumping to memory table
  - Make sure to throw proper exceptions
- Range Iterator API
- Optimize (or warn against) use of .Keys/.Values, and maybe provide alternate IterKeys/IterValues that is a plain enumerable without count
- Async merge
- Value-Stream interface
- Make SSTBuilder write non-interned values immidiately
- Better level size curves
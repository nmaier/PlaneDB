# ![Icon](https://github.com/nmaier/PlaneDB/raw/master/icon.png) PlaneDB.Remote

Serve PlaneDBs over the network.

![.NET CI Status](https://github.com/nmaier/PlaneDB/workflows/.NET%20CI/badge.svg)
![Nuget](https://img.shields.io/nuget/v/NMaier.PlaneDB)
![GitHub](https://img.shields.io/github/license/nmaier/PlaneDB)

## Features

- General purpose binary protocol with client and server implementations.
- Partial redis protocol server implementation, for interoperability with existing redis clients.
  - Most basic redis operations are implemented, as well as lists and sets.
  - Blocking list operations, hashes, sorted sets, HLLs and pub/sub are not implemented (yet)

## Status

Beta-grade software.
I have been dogfooding the code for a while in various internal applications of mine (none business-critical), with millions of records, along with running the unit test suite a lot.

See [docs/remote.md](../docs/remote.md) for the protocol specification including security considerations, threat model and authentication scheme.

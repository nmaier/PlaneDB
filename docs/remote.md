# Remote protocol

The remote protocol, contained in the `NMaier.PlaneDB.Remote` package, is meant to provide the necessary on-wire specification to implement remote PlaneDB "proxies" sharing the same interfaces as normal in-process PlaneDB instances.

The protocol flow is as follows.

1. Authentication
2. Database selection
3. Canary seed exchange
4. Command handling

To make it easier to implement this parts, some data types and their on-wire representation are specified first.

An alternative is the `NMaier.PlaneDB.RedisServer` package, implementing a partial redis-protocol compatible server that can be access using most redis-protocol compliant client software.

## Security considerations and threat model

For simplicity everything that happens after a successful mutual authentication will be considered "trusted": While basic error checking will be performed, the protocol and implementation will not protect against data manipulation, denial of service or other attacks.

It is therefore paramount to actually make sure trusted parties are trusted, as such parties can:

- Read all data.
- Potentially modify data (unless the DB is read-only).
- Perform (distributed) denial of service attacks, that can be virtually indistinguishable from normal use.
- Modify data in such a way that other clients processing the data will encounter denial of service or even remote code execution issues (depending on the serializers in use; the default serializers are not prone to RCE, but DoS is a possibility).

Using the protocol without TLS therefore implies trust in the network environment. It is therefore recommended to always use TLS. TLS also protect against a lot more on-wire data corruption errors. like certain types of bit flips not addressed by TCP checksums.

Providing additional ACL schemes is outside of the scope of this protocol.

The implementation must furthermore make reasonable attempts to protect itself against denial of service attack, such as slow senders and/or large array transmissions.

## Data types

The basic protocol defines a few on-wire data types. Each type is identified by a single byte designating the type. Higher-level command handlers can and will verify that a specific value on the wire has the type it expects at that point.

### `bool (0x01)`

True (not `0x00`) or false (`0x00`), represented as a single byte.

### `int (0x2)`

A 32-bit integer value in little endian.

### `array (0x2)`

An array ia a collection of bytes with a length.

- Length in bytes as a 32-bit little-ending integer.
- The bytes, if any.

Zero-length arrays will not send any bytes beyond the length.

As a special case a length of exactly `-1` represents no-result/null.

Any other negative length values are errors.

### `error (0x3)`

An error with type and (optional) message.

The error type is encoded as a single byte, with registered values being:

- `0x1` - General error
- `0x2` - I/O error
- `0x3` - ReadOnly error. The database is readonly and an attempt to modify data was made.
- `0x4` - Not supported.
- `0x5` - Not implemented (something in the PlaneDB API that was not (yet) implemented in the remote interface).

It is followed by an UTF-8 encoded string representing the error message. This message string may be empty.

## Authentication

Authentication is performed by a simple cryptographic hashing scheme to establish trust between the server and client.

This option was selected because it is easy to implement and reasonably easy to understand, and performs quite well.

0. Both server and client know a shared secret. How this shared secret is agreed upon is outside of the scope of this scheme.
1. The server selects a random generated `nonce(Server)` (use only once value), where the randomness must come from a defacto-unguessable such as a cryptographically-strong PRNG. It must not be guessable and should not be reused if possible to minimize the risk of replay attacks. To make replay attacks even harder, the nonce shall include the current system timestamp as a reasonable resolution.
2. The server sends this `nonce(Server)` to the client.
3. The client takes a cryptographic `MAC` of `nonce(Server)` with the shared secret as the key, and sends back the result to the server.
4. The server performs the same computation, and checks with a constant-time method if the client-supplied values matches its own computation. If it does, the client is authenticated.
5. The client produces a `nonce(Client)` in the same way, and authenticates the server the same way.

### Risks/Weaknesses

This scheme relies on a couple of things:

- A secure source of randomness
- A secure cryptographic MAC.

With all such schemes, there is a chance of `nonce` reuse which would enable replay attacks. This is mostly mitigated by mixing in the current time into the secret, i.e. a quasi-monotonically increasing counter along with the randomness. There is still a chance the clock resets to a previous time (e.g. by an ntp client to fix local drift). Still, it's rather improbable that the attacked side will compute the same random `nonce` after such a clock reset. To further minimize the risk here, and also the lower the probability of the attacker just getting *extremely* lucky and guessing the `MAC` correctly in one attempt, the authentication protocol can be repeated.

### Implementation

The current version of the protocol uses `HMAC-SHA256` as the MAC, an `nonces` of 64-byte (512 bit) containing 56 bytes of randomness (448 bit) along with a 64 bit timestamp in `.NET` ticks ("A single tick represents one hundred nanoseconds or one ten-millionth of a second").

Additional, to make replay attacks less probable, the authentication is performed twice (so two `nonces` with two distinct values).

1. Server sends the protocol version `4` as a single byte `array`.
2. Server sends its nonce in an `array`.
3. Client reads the protocol version, and aborts if not `3`.
4. Client reads nonce and sends back the `MAC` as an `array`.
5. Client sends its `nonce`.
6. Server receives `MAC` and aborts on mismatch.
7. Server received client `nonce` and sends back the `MAC` as an `array`.
8. Client receives `MAC` and aborts on mismatch.
9. If not aborted, then client considers server authenticated, and server considers client authenticated.

## Database selection

`PlaneDB.Remote` supports making available multiple databases via the same TCP listener. In order to facilitate this, each client must provide an `array` containing the UTF-8 encoded moniker to identify which DB it likes to access right after authentication is completed.

Specifying a moniker that is not known to the server results in an error.

## Canary seed exchange

The protocol uses "canary" values to detect accidental and/or intentional corruption. To facilitate this, a simple PRNG is seeded with a common value. This PRNG will then later produce canary values to be inserted and checked between commands.

The server randomly chooses a 32-bit seed, encodes it little-endian and sends this over the wire immediately following a successful mutual authentication. The client receives the seed and seeds its PRNG accordingly.

Both sides use the same PRNG implementation in will therefore produce the same sequence of values according to the shared seed.

## Generic command protocol

After successful authentication a client may issue one or more commands, one at a time.

Generally, the available commands correspond closely to the public API interface of a PlaneDB (with some notable exceptions).

1. For each command, the client first computes the next canary using the canary-PRNG, a 32-bit value, and sends it over the wire.

2. This is followed by a command code, a 32-bit little-endian integer.

3. The client encodes parameters to the command using the protocol data types, if any, and sends the encoded values.

4. The server decodes the data types, and responds with an intermediary or final result, or an error.

5. If the result is an error, the connection is considered tainted and should not be used to issue more commands.

6. If the result is final, command processing is done, and a new command can be accepted.

7. If the result is intermediate, the client at this point can provide any information the server might have requested to continue, or it might read more results immediately (e.g during enumerations).

## `fail (0x01)` command

This provokes a deliberate failure.

This command has no arguments.

The server should always respond with an error.

## `count (0x02)` command

No arguments.

The server shall respond with an `int` containing the number of elements in the database.

## `readonly (0x03)` command

No arguments.

The server shall respond with a `bool` indicating whether the database is read-only.

## `addorupdate (0x04)` command

Arguments:

1. Key to update (`array`)

The server will respond with a `bool`, indicating whether the value will be updated (`false`) or added (`true`), as an intermediate result. If the value is going to be updated, the server will immediately send another intermediate result, containing the current value in an `array`.

The client responds with an `array` containing the new value.

The server responds with the final result which is always a `bool` of value `true`, to ensure errors are detected early.

## `clear (0x05)` command

No arguments.

The server will respond with a `bool`, indicating whether the database was cleared. Right now, this is always `true`, as a readonly database would raise an error instead.

## `containskey (0x06)` command

Arguments:

1. Key to check (`array`)

The server will respond with a `bool` indicating wether the database contains said key.

## `getoradd (0x07)` command

Arguments:

1. Key to get or add (`array`)

The client send the key, to which the server will respond with an `array`. If that `array` is not a `no-result` array, it's the result of the get, and thus the final result.

If the server responds with `no-result` the client next sends the value for the key as an `array`.

The server responds with the final result which is always a `bool` of value `true`, indicating the key was added.

## `remove (0x08)` command

Arguments:

1. Key to remove (`array`)

The client send the key, to which the server will respond with a `bool` indicating whether the key way removed or not (in case it did not exist).

## `set (0x09)` command

Arguments:

1. Key to set (`array`)
1. Value to associate with key (`array`)

The client sends the key and value, to which the server will respond with a `bool` indicating the key was set. This is value is currently always `true`, as errors are transmitted as `error`s.

## `tryadd (0x0a)` command

Arguments:

1. Key to add (`array`)
1. Value to associate with key (`array`)

The client sends the key and value, to which the server will respond with a `bool` indicating the key was added, or not (if it already existed).

## `tryadd2 (0x0b)` command

Arguments:

1. Key to add (`array`)
1. Value to associate with key (`array`)

The client sends the key and value, to which the server will respond with an `array` containing the existing value (in which case the key was not added). If the key was added, the result will be the `no-result array`, indicating there was no existing value for the key.

## `trygetvalue (0x0c)` command

Arguments:

1. Key to query (`array`)

The client sends the key to query, to which the server will respond with either `no-result` or the value (`array`).

## `tryremove (0x0d)` command

Arguments:

1. Key to remove (`array`)

The client sends the key to remove, to which the server will respond with either `no-result`, indicating the key was not present, or the value associated with the key before removal (`array`).

## `tryupdate (0x0e)` command

Arguments:

1. Key to remove (`array`)
2. The value to set (`array`)
3. The value expected to currently be associated with the key (`array`)

The client sends the key to update, followed by the value and the expected value, to which the server will respond with a `bool` indicating whether the value was updated or not (if the key was not found, or the expected value did not match). This is a basic Compare-and-swap (CAS) scheme.

## `enumerate (0x0f)` command

No arguments.

The server will respond with one a key (`array`) followed by the value associated with it (`array`) for each such pair in the database, the moment the command was received. Later updates to the DB while the enumeration is still in progress will not be reflected. The enumeration ends when the server sends `no-result` for the key.

## `enumeratekeys (0x10)` command

No arguments.

The server will respond with one a key (`array`) each key in the database at the moment the command was received. Later updates to the DB while the enumeration is still in progress will not be reflected. The enumeration ends when the server sends `no-result` for the key.

## `tablespace (0x11)` command

No arguments.

The server will respond with an utf-8 encoded string containing the table space associated with the connection (`array`).

## `flush (0x12)` command

No arguments.

The server will flush all data to disk and then respond with a `bool` that is always `true`.

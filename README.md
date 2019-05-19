# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNjM5MDM2MjYyMTE2LWU3ZTczNjA4YTAxZGNhMzk2MDc1MDViZTE0MzkyMmI2Y2E0OGE1ODg0MGJiZjY3YzY3MTE2MTA4MDcxZmMzMzY

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://travis-ci.com/simerplaha/SwayDB.svg?branch=master
[build-link]: https://travis-ci.com/simerplaha/SwayDB

Embeddable persistent and in-memory database.

Documentation: http://swaydb.io

## Performance

| Storage  type   | Performance                               
|:---------------:|:------------------------------------------------------
| Persistent      | up to `308,000` writes/sec & `316,000` reads/sec                
| In memory       | up to `653,000` writes/sec & `628,000` reads/sec                

View detailed benchmark results [here](http://swaydb.io/performance/macbook-pro-mid-2014/memory). 

## Features

- Non-blocking.
- Single or multiple disks persistent, in-memory or periodically persistent.
- Atomic updates and inserts.
- APIs similar to Scala collections.
- Auto expiring key-value ([TTL](http://www.swaydb.io/api/write-api/expire/))
- Range [update, remove & expire](http://www.swaydb.io/api/write-api/)
- Key only iterations (Lazily fetched values)
- Supported data types 
    - `Map[K, V]`
        - Nested maps similar to `Tables` with [extensions](http://www.swaydb.io/extending-databases/). 
    - `Set[T]`
- In-built custom serialization API with [Slice](http://www.swaydb.io/slice/byte-slice/) 
- [Configurable Levels](http://www.swaydb.io/configuring-levels/)
- Configurable [cacheSize](http://www.swaydb.io/configuring-levels/cacheSize/)
- Concurrent level compaction
- Duplicate values can be detected and written only ones with the configuration [compressDuplicateValues](http://www.swaydb.io/configuring-levels/compressDuplicateValues/).
- Compression with [LZ4](https://github.com/lz4/lz4-java) & [Snappy](https://github.com/xerial/snappy-java) are fully supported
for both [Persistent](http://www.swaydb.io/create-databases/persistent/) & [Memory](http://www.swaydb.io/create-databases/memory/) databases for each Level.
- All LZ4 instances, compressors & decompressors are [configurable](http://www.swaydb.io/configuring-levels/groupingStrategy/).
    - LZ4 Instances - `FastestInstance`, `FastestJavaInstance`, `NativeInstance`, `SafeInstance` & `UnsafeInstance`.
    - LZ4 Compressors - `FastCompressor` & `HighCompressor`.
    - LZ4 Decompressors - `FastDecompressor` & `SafeDecompressor`.
- Optional Memory-mapped files
- Scala Streams
- Bloom filters

[Read more](http://swaydb.io/).

## Demo Streaming API
```scala
//Iteration: fetch all key-values withing range 10 to 90, update values and batch write updated key-values
db
  .from(10)
  .takeWhile {
    case (key, value) =>
      key <= 90
  }
  .map {
    case (key, value) =>
      (key, value + "_updated")
  }
  .materialize
  .flatMap(db.put) //write updated key-values to database
```
## Quick start
[Quick start demo](http://swaydb.io/quick-start).

## Examples 
- [Creating Tables](http://swaydb.io/examples/creating-tables)
- [Event-sourcing](http://swaydb.io/examples/event-sourcing)
- [Storing data in chunks](http://swaydb.io/examples/storing-data-in-chunks)

## Related GitHub projects
- [SwayDB.examples](https://github.com/simerplaha/SwayDB.examples) - Examples demonstrating features and APIs.
- [SwayDB.benchmark](https://github.com/simerplaha/SwayDB.benchmark) - Benchmarks for write and read performance.
- [SwayDB.stress](https://github.com/simerplaha/SwayDB.stress) - Stress tests.
- [SwayDB.io](https://github.com/simerplaha/SwayDB.io) - Website code.

# Project status

Undergoing integration, stress & performance testing. 

# Master branch  status

Development is done directly on master. So at times it will be unstable. 
See the tags to get a more stable version.

# Project support
Thank you

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>
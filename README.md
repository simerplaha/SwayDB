# <img src="docs/logo.png" align = "right"/> SwayDB [![Gitter Chat][gitter-badge]][gitter-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

SwayDB is a type-safe & non-blocking key-value storage library for single/multiple disks and in-memory storage.

It's an implementation of [Log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) 
written in Scala with asynchronous Leveled Compaction based on push-pull strategy built on the 
[Actor model](https://en.wikipedia.org/wiki/Actor_model).

It supports configurable [Graph like](http://www.swaydb.io/implementation/segment/group/) 
file format for faster reads and has compression support for both [memory](http://www.swaydb.io/create-databases/memory/) 
& [persistent](http://www.swaydb.io/create-databases/persistent/) databases.

Documentation: http://swaydb.io

## Performance

| Storage  type   | Performance                               
|:---------------:|:------------------------------------------------------
| Persistent      | up to `308,000` writes/sec & `316,000` reads/sec                
| In memory       | up to `653,000` writes/sec & `628,000` reads/sec                

View detailed benchmark results [here](http://swaydb.io/performance/macbook-pro-mid-2014/memory). 

## Features

- Embeddable, Type-safe, non-blocking
- Single or multiple disks persistent, in-memory or periodically persistent
- ACID like atomic writes with [Batch API](http://www.swaydb.io/api/write-api/batch/)
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
- Fault tolerant

[Read more](http://swaydb.io/).

## Demo API
```scala
//Iteration: fetch all key-values withing range 10 to 90, update values and batch write updated key-values
db
  .from(10)
  .tillKey(_ <= 90)
  .map {
    case (key, value) =>
      (key, value + "_updated")
  } andThen {
     updatedKeyValues =>
       db.batchPut(updatedKeyValues)
  }
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

# Status: Beta

Undergoing integration, stress & performance testing.

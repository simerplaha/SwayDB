# <img src="docs/logo.png" align = "right"/> SwayDB [![Gitter Chat][gitter-badge]][gitter-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

Type-safe & non-blocking key-value storage library for single/multiple disks and in-memory storage.

Documentation: http://swaydb.io

## Performance

| Storage  type   | Performance                               
|:---------------:|:------------------------------------------------------
| Persistent      | up to `308,000` writes/sec & `316,000` reads/sec                
| In memory       | up to `653,000` writes/sec & `628,000` reads/sec                

View detailed benchmark results [here](http://swaydb.io/performance/macbook-pro-mid-2014/memory). 

## Features

- Embeddable, Type-safe, non-blocking
- Multiple disks, In-memory & periodically persistent
- ACID like atomic writes with [Batch API](http://www.swaydb.io/api/write-api/batch/)
- APIs similar to Scala collections.
- Key only iterations (Lazily fetched values)
- Data storage formats
    - Key-value (`Map[K, V]`)
    - Row (`Set[T]`)
- In-built custom serialization API with [Slice](http://www.swaydb.io/slice/byte-slice/) 
- Configurable Levels
- Configurable cache size
- Concurrent level compaction
- Optional Memory-mapped files
- Scala Streams
- Bloom filters
- Fault tolerant

[Read more](http://swaydb.io/).

## Demo
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

## Backward compatibility
Currently for versions `0.*` the code & file formats are going through rapid changes.
Next few versions will require many changes in the file formats to improve data compression.

Backward compatible will be supported for versions `1.*` and after. 
So please suggest features & improvements now while we are still in version `0.*`.
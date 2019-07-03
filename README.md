# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNjM5MDM2MjYyMTE2LWU3ZTczNjA4YTAxZGNhMzk2MDc1MDViZTE0MzkyMmI2Y2E0OGE1ODg0MGJiZjY3YzY3MTE2MTA4MDcxZmMzMzY

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://travis-ci.com/simerplaha/SwayDB.svg?branch=develop
[build-link]: https://travis-ci.com/simerplaha/SwayDB

Embeddable persistent and in-memory database for resource efficiency, performance 
and easy data management with simple collections API.

[Documentation - SwayDB.io](http://swaydb.io).

## Performance

| Storage  type   | Performance                               
|:---------------:|:------------------------------------------------------
| Persistent      | up to `308,000` writes/sec & `316,000` reads/sec                
| In memory       | up to `653,000` writes/sec & `628,000` reads/sec                

View detailed benchmark results [here](http://swaydb.io/performance/macbook-pro-mid-2014/memory). 

## Features

- [Java](https://github.com/simerplaha/SwayDB.java) & [Kotlin](https://github.com/simerplaha/SwayDB.kotlin) wrappers.
- Single or multiple disks persistent, in-memory or periodically persistent.
- Simple Stream based iteration following Scala collections APIs.
- Atomic updates and inserts.
- Custom updates using [JVM function](http://www.swaydb.io/api/write/registerFunction/).
- TTL - auto [expiring](http://www.swaydb.io/api/write/expire/) key-values.
- Range [update, remove & expire](http://www.swaydb.io/api/write/update-range/).
- Key only iterations (Lazily fetched values).
- Supported data types - `Map[K, V]` & `Set[T]`.
- [Configurable](http://www.swaydb.io/configuring-levels/) levels.
- [Compression](http://www.swaydb.io/configuring-levels/groupingStrategy/) for both Persistent & Memory databases with LZ4 & Snappy.
- Duplicate values can be eliminated with [compressDuplicateValues](http://www.swaydb.io/configuring-levels/compressDuplicateValues/).

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

## Related GitHub projects
- [SwayDB.examples](https://github.com/simerplaha/SwayDB.examples) - Examples demonstrating features and APIs.
- [SwayDB.benchmark](https://github.com/simerplaha/SwayDB.benchmark) - Benchmarks for write and read performance.
- [SwayDB.stress](https://github.com/simerplaha/SwayDB.stress) - Stress tests.
- [SwayDB.io](https://github.com/simerplaha/SwayDB.io) - Website code.

# Project status

Undergoing frequent changes & beta testing.

See the tags to see a more stable version.
Default branch is `develop` and is pushed to directly so at times it will be unstable. 

# Project support
Thank you Jetbrains for providing an open-source licence for their awesome development tools. 

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>
# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://travis-ci.com/simerplaha/SwayDB.svg?branch=master
[build-link]: https://travis-ci.com/simerplaha/SwayDB

Embeddable persistent and in-memory database. Resource efficiency, performance 
and easy data management with simple functional APIs.

[See documentation - SwayDB.io](http://swaydb.io) (currently being updated for latest version).

## Performance (old benchmark for v0.2 - [#119](https://github.com/simerplaha/SwayDB/issues/119))

| Storage  type   | Performance                               
|:---------------:|:------------------------------------------------------
| Persistent      | up to `308,000` writes/sec & `316,000` reads/sec                
| In memory       | up to `653,000` writes/sec & `628,000` reads/sec                

View detailed benchmark results [here](http://swaydb.io/performance/macbook-pro-mid-2014/memory). 

## Overview

- [Java](http://swaydb.io/quick-start/?language=java/) & [Kotlin](https://github.com/simerplaha/SwayDB.kotlin) APIs.
- Single or multiple disks persistent, in-memory or periodically persistent.
- Simple data types - `Map[K, V]` & `Set[T]`.
- Simple Stream based iteration following collections APIs.
- Atomic updates and inserts with [transactions](http://swaydb.io/api/write/transaction/?language=scala/).
- Custom updates using [JVM function](http://www.swaydb.io/api/write/registerFunction/).
- TTL - auto [expiring](http://www.swaydb.io/api/write/expire/) key-values.
- Range [update, remove & expire](http://www.swaydb.io/api/write/update-range/).
- Non-blocking with customisable non-blocking or blocking APIs.
- Key only iterations (Lazily fetched values).
- [Configurable compression](http://swaydb.io/configuring-levels/compressionStrategy/?language=scala/) with LZ4 & Snappy
- [Configurable](http://www.swaydb.io/configuring-levels/) core internals.
- Duplicate values can be eliminated with [compressDuplicateValues](http://www.swaydb.io/configuring-levels/compressDuplicateValues/).
- A small type-safe [Actor](http://swaydb.io/actor/?language=scala/) implementation.
- [IO](http://swaydb.io/io/?language=scala/) type for type-safe error handling. 

[Read more](http://swaydb.io/).

## Quick start
[Quick start App](http://swaydb.io/quick-start/?language=scala/).

## Project status 
Undergoing beta & performance testing. Backward binary compatibility is not yet a priority for minor releases unless it's requested.

## Related GitHub projects
- [SwayDB.java.examples](https://github.com/simerplaha/SwayDB.java.examples) - Java examples demonstrating features and APIs.
- [SwayDB.kotlin.examples](https://github.com/simerplaha/SwayDB.kotlin.examples) - Kotlin examples demonstrating features and APIs.
- [SwayDB.scala.examples](https://github.com/simerplaha/SwayDB.scala.examples) - Scala examples demonstrating features and APIs.
- [SwayDB.benchmark](https://github.com/simerplaha/SwayDB.benchmark) - Benchmarks for write and read performance.
- [SwayDB.stress](https://github.com/simerplaha/SwayDB.stress) - Stress tests.
- [SwayDB.website](https://github.com/simerplaha/SwayDB.website) - Website code.

## Contribution
Contributions are welcomed following the [Scala code of conduct](https://www.scala-lang.org/conduct/).

# Project support
Thank you Jetbrains for providing an open-source licence for their awesome development tools. 

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>
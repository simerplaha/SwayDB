# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://api.travis-ci.com/simerplaha/SwayDB.svg?branch=master
[build-link]: https://travis-ci.com/simerplaha/SwayDB

Embeddable persistent and in-memory database aimed at:

- **Simple data types** - `Map` & `Set` (see [#47](https://github.com/simerplaha/SwayDB/issues/47) for `List` & `Queue`)
- **High performance** - Fast writes & reads.
- **Resource efficiency** - Reduce server costs (Non-blocking, high compression, low IOps, low GC footprint)

[Documentation - SwayDB.io](http://swaydb.io).

[Project status](#Project-status).

## Performance

| Storage  type  | Performance                               
|:---------------|:------------------------------------------------------
| Persistent     | up to `863,000` reads/sec & `482,000` writes/sec                 
| Memory         | TODO                

View comparable benchmarks [here](http://swaydb.io/benchmarks/rocksdb/?language=scala/). 

## Overview

- [Scala](https://github.com/simerplaha/SwayDB.scala.examples), [Java](http://swaydb.io/quick-start/?language=java/) & [Kotlin](https://github.com/simerplaha/SwayDB.kotlin).
- Single or multiple disks persistent, in-memory or periodically persistent.
- Simple data types - `Map[K, V]` & `Set[T]`.
- Streaming.
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
Your feedback and review is very important to get to production. Please get involved via
chat, issues or email which is on my [profile](https://github.com/simerplaha). 

Undergoing beta & performance testing. Backward binary compatibility is not yet a priority for minor releases 
unless it's requested.

See tasks labelled [Production release](https://github.com/simerplaha/SwayDB/labels/Production%20release) that are 
required before becoming production ready. 

## Related GitHub projects
- [SwayDB.java.examples](https://github.com/simerplaha/SwayDB.java.examples) - Java examples demonstrating features and APIs.
- [SwayDB.kotlin.examples](https://github.com/simerplaha/SwayDB.kotlin.examples) - Kotlin examples demonstrating features and APIs.
- [SwayDB.scala.examples](https://github.com/simerplaha/SwayDB.scala.examples) - Scala examples demonstrating features and APIs.
- [SwayDB.benchmark](https://github.com/simerplaha/SwayDB.benchmark) - Performance benchmarks.
- [SwayDB.website](https://github.com/simerplaha/SwayDB.website) - Website code.

## Contribution
Contributions are welcomed following the [Scala code of conduct](https://www.scala-lang.org/conduct/).

# Sponsors
Thank you Jetbrains for providing an open-source licence for their awesome development tools. 

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>
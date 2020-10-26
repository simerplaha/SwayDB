# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://github.com/simerplaha/SwayDB/workflows/Build/badge.svg
[build-link]: https://github.com/simerplaha/SwayDB/actions

Embeddable **persistent** and **in-memory** key-value storage engine aimed for **high performance** & **resource efficiency**. 

_High level goal is to be efficient at **managing bytes** on-disk and in-memory by recognising reoccurring patterns 
in serialised bytes without restricting the **core** implementation to any specific data model (SQL, No-SQL, Queue etc) 
or storage type (Disk or RAM). The core provides many configurations that can be manually tuned for custom use-cases, 
but we aim to eventually implement automatic runtime tuning when we are able to collect and analyse runtime machine
statistics & read-write patterns._

Manage data by creating familiar typed data structures like `Map`, `Set`, `Queue` `MultiMap`, `SetMap` that can easily be converted 
to native Java and Scala collections.
  
Perform conditional updates/data modifications with any **Java**, **Scala**, **Kotlin** or any **native JVM** code - **No query language**.

__Scalable on a single machine__ - Data can be stored on multiple local __HDD/SSD__ and, single or multiple __Threads__ 
can be allocated for reads, caching & compaction.

SwayDB's core is **non-blocking**, but the APIs are configurable to be blocking, non-blocking and/or reactive.   

Configurable to suit **different workloads**. Some use-cases are:
- General key-value storage
- Message queues
- Time-series or Events data
- Caching
- Application logs
- Archiving data or cold storage with high file level compression

See [comparable benchmarks](http://swaydb.io/benchmarks/rocksdb/?language=scala/) with RocksDB or QuickStart in 
[Java](http://swaydb.io/quick-start/?language=java&data-type=map&functions=off), 
[Scala](http://swaydb.io/quick-start/?language=scala&data-type=map&functions=off) or 
[Kotlin](https://github.com/simerplaha/SwayDB.kotlin.examples/blob/master/src/main/kotlin/quickstart/QuickStartMapSimple.kt).

[Documentation](http://swaydb.io) | [License summary](#license-summary) | [Project status](#Project-status)

## Performance

| Storage  type  | Performance                               
|:---------------|:------------------------------------------------------
| Persistent     | up to `863,000` reads/sec & `482,000` writes/sec                 
| Memory         | TODO                


## Overview

- Simple data types - `Map`, `Set`, `Queue`, `SetMap` & `MultiMap`.
- Single or multiple disks persistent, in-memory or periodically persistent.
- Streaming.
- Atomic updates and inserts with [transactions](http://swaydb.io/api/write/transaction/?language=scala/).
- Custom updates using any [JVM function](http://www.swaydb.io/api/write/registerFunction/).
- TTL - auto [expiring](http://www.swaydb.io/api/write/expire/) key-values.
- Range [update, remove & expire](http://www.swaydb.io/api/write/update-range/).
- Non-blocking with customisable non-blocking or blocking APIs.
- Key only iterations (Lazily fetched values).
- [Configurable compression](http://swaydb.io/configuration/compressions/?language=scala) with LZ4 & Snappy
- [Configurable](http://swaydb.io/configuration/?language=scala) core internals.
- Duplicate values can be eliminated with [compressDuplicateValues](http://swaydb.io/configuration/valuesConfig/?language=scala).
- A small type-safe [Actor](http://swaydb.io/actor/?language=scala/) implementation.
- [IO](http://swaydb.io/io/?language=scala/) type for type-safe error handling. 

[Read more](http://swaydb.io/).

## Quick start

- [Java - Quick start](http://swaydb.io/quick-start/?language=java&data-type=map&functions=off).
- [Scala - Quick start](http://swaydb.io/quick-start/?language=scala&data-type=map&functions=off).
- [Kotlin - Quick start](https://github.com/simerplaha/SwayDB.kotlin.examples/blob/master/src/main/kotlin/quickstart/QuickStartMapSimple.kt).

## Project status 
Your feedback and review is very important to get to production. Please get involved via
chat, issues or email which is on my [profile](https://github.com/simerplaha). 

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

## License summary

We would like to see others find SwayDB useful in their **own projects**, **companies** and other **open-source projects** 
for both **personal** and **commercial** reasons, the license **only** asks for your modifications (e.g bug-fixes) 
to SwayDB's source code to be shared so that it supports the contributors by not duplicating efforts and shares knowledge on
this project's subject.

The language in the [LICENSE](/LICENSE.md) file follows [GNU's FAQ](https://www.gnu.org/licenses/gpl-faq.en.html#GPLIncompatibleLibs).

# Sponsors
Thank you Jetbrains for providing an open-source licence for their awesome development tools. 

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>

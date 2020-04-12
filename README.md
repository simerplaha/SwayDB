# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg
[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg
[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://github.com/simerplaha/SwayDB/workflows/Build/badge.svg
[build-link]: https://github.com/simerplaha/SwayDB/actions

Fast embeddable **persistent** and **in-memory** key-value storage engine that provides storage 
as simple data structures - `Map`, `Set` & `Queue`.

Conditional updates/data modifications can be performed by simply submitting any **Java**, **Scala**, **Kotlin** or any 
**native JVM** code. 

__Scalable on a single machine__ - Data can be stored on multiple local __HDD/SSD__ and, single or multiple __Threads__ 
can be allocated for reads, caching & compaction.

SwayDB's configurations can be tuned for **various workloads**. Some use-cases are:
- General key-value storage
- Message queues
- Time-series or Events data
- Application logs
- Archiving data or cold storage with high file level compression

See [comparable benchmarks](http://swaydb.io/benchmarks/rocksdb/?language=scala/) with RocksDB or QuickStart in 
[Java](http://swaydb.io/quick-start/?language=java&starter=functionsOff), [Scala](http://swaydb.io/quick-start/?language=scala&starter=functionsOff) 
or [Kotlin](https://github.com/simerplaha/SwayDB.kotlin.examples/blob/master/src/main/kotlin/quickstart/QuickStartMapSimple.kt).

[Documentation](http://swaydb.io) | [License summary](#license-summary) | [Project status](#Project-status)

## Performance

| Storage  type  | Performance                               
|:---------------|:------------------------------------------------------
| Persistent     | up to `863,000` reads/sec & `482,000` writes/sec                 
| Memory         | TODO                


## Overview

- Simple data types - `Map[K, V]`, `Set[A]` & `Queue[A]`.
- [Java](http://swaydb.io/quick-start/?language=java&starter=functionsOff), [Scala](http://swaydb.io/quick-start/?language=scala&starter=functionsOff) & [Kotlin](https://github.com/simerplaha/SwayDB.kotlin.examples).
- Single or multiple disks persistent, in-memory or periodically persistent.
- Streaming.
- Atomic updates and inserts with [transactions](http://swaydb.io/api/write/transaction/?language=scala/).
- Custom updates using any [JVM function](http://www.swaydb.io/api/write/registerFunction/).
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

- [Java - Quick start](http://swaydb.io/quick-start/?language=java&starter=functionsOff).
- [Scala - Quick start](http://swaydb.io/quick-start/?language=scala&starter=functionsOff).
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

Would like to see others find SwayDB useful in their **own projects**, **companies** and other **open-source projects** 
for both **personal** or **commercial** reasons, the license **only** asks for your modifications (e.g bug-fixes) 
to SwayDB's source code to be shared so that it supports the contributors by not duplicating efforts. 
You **do not have to** share your program's code or are bounded by AGPLv3 as long as you share your modifications. 

If you have any questions or think that the current license restricts you in any way please get in touch.

The language in the [LICENSE](/LICENSE.md) file follows [GNU's FAQ](https://www.gnu.org/licenses/gpl-faq.en.html#GPLIncompatibleLibs)
so that it is correct in legal terms.

# Sponsors
Thank you Jetbrains for providing an open-source licence for their awesome development tools. 

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png" 
alt="Jetbrains support" height="150" border="10" /></a>

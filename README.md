# <img src="docs/logo.png" align = "right"/> SwayDB [![Slack Chat][slack-badge]][slack-link] [![Gitter Chat][gitter-badge]][gitter-link] [![Build status][build-badge]][build-link] [![Maven central][maven-badge]][maven-link]

[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg

[gitter-link]: https://gitter.im/SwayDB-chat/Lobby

[slack-badge]: https://img.shields.io/badge/slack-join%20chat-e01563.svg

[slack-link]: https://join.slack.com/t/swaydb/shared_invite/enQtNzI1NzM1NTA0NzQxLTJiNjRhMDg2NGQ3YzBkNGMxZGRmODlkN2M3MWEwM2U2NWY1ZmU5OWEyYTgyN2ZhYjlhNjdlZTM3YWJjMGZmNzQ

[maven-badge]: https://img.shields.io/maven-central/v/io.swaydb/swaydb_2.12.svg

[maven-link]: https://search.maven.org/search?q=g:io.swaydb%20AND%20a:swaydb_2.12

[build-badge]: https://github.com/simerplaha/SwayDB/workflows/Build/badge.svg

[build-link]: https://github.com/simerplaha/SwayDB/actions

**Persistent** and **in-memory** key-value storage engine for the JVM.

**Status**: Under testing & performance optimisations. See [project status](#Project-status).

[Documentation](http://swaydb.io)

## Overview

- Data types - `Map`, `Set`, `Queue`, `SetMap` & `MultiMap` with native Java and Scala collections support.
- Conditional updates using any pure [JVM function](http://swaydb.io/api/pure-functions/?language=java) - **No query
  language**.
- Atomic updates and inserts with `Transaction`.
- **Non-blocking core** with configurable APIs for blocking, non-blocking and/or reactive use-cases.
- Single or multiple disks persistent, in-memory or eventually persistent.
- Streaming support for forward and reverse iterations.
- TTL - auto [expiring](http://www.swaydb.io/api/write/expire/) key-values.
- Range operations to update, remove & expire data.
- Key only iterations (Lazily fetched values).
- [Configurable compression](http://swaydb.io/configuration/compressions/?language=scala) with LZ4 & Snappy
- Highly configurable core internals to support custom workloads.
- Duplicate values elimination
  with [compressDuplicateValues](http://swaydb.io/configuration/valuesConfig/?language=scala).

## Use cases

Highly configurable to suit **different workloads**. Some known use-cases are:

- General key-value storage
- Message queues
- Time-series or Events data
- Caching
- Application logs
- Archiving data or cold storage with high file level compression

## Quick start

- [Java - Quick start](http://swaydb.io/quick-start/?language=java&data-type=map&functions=off).
- [Scala - Quick start](http://swaydb.io/quick-start/?language=scala&data-type=map&functions=off).
- [Kotlin - Quick start](https://github.com/simerplaha/SwayDB.kotlin.examples/blob/master/src/main/kotlin/quickstart/QuickStartMapSimple.kt)
  .

## Contributing

Contributions are encouraged and welcomed.

**Code of conduct** - Be nice, welcoming, friendly & supportive of each other. Follow the Apache
foundation's [COC](https://www.apache.org/foundation/policies/conduct.html).

- **Contributing to data management API**
    - Build new data structures like Graph, List, Geospatial, Logs, Observables etc on top of existing data structures.
      See [MultiMap](https://github.com/simerplaha/SwayDB/blob/master/swaydb/src/main/scala/swaydb/MultiMap.scala) for
      example which is an extension
      of [Map](https://github.com/simerplaha/SwayDB/blob/master/swaydb/src/main/scala/swaydb/Map.scala).
    - Test and improve existing data structures.

- **Contributing to core API**
    - See issues
      labelled [good first issue](https://github.com/simerplaha/SwayDB/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
      .

- **Contributing to core internals**
    - See code marked `TODO`.

## Project status

Your feedback and review is very important to get to production. Please get involved via chat, issues or email which is
on my [profile](https://github.com/simerplaha).

Undergoing performance optimisations. Future releases might not be backward compatible until we are production ready.

See tasks labelled [Production release](https://github.com/simerplaha/SwayDB/labels/Production%20release)
that are required before becoming production ready.

## Related GitHub projects

- [SwayDB.java.examples](https://github.com/simerplaha/SwayDB.java.examples) - Java examples demonstrating features and
  APIs.
- [SwayDB.kotlin.examples](https://github.com/simerplaha/SwayDB.kotlin.examples) - Kotlin examples demonstrating
  features and APIs.
- [SwayDB.scala.examples](https://github.com/simerplaha/SwayDB.scala.examples) - Scala examples demonstrating features
  and APIs.
- [SwayDB.benchmark](https://github.com/simerplaha/SwayDB.benchmark) - Performance benchmarks.
- [SwayDB.website](https://github.com/simerplaha/SwayDB.website) - Website code.

# Sponsors

Thank you Jetbrains for providing an open-source licence for their awesome development tools.

<a href="https://www.jetbrains.com/?from=SwayDB" target="_blank"><img src="/docs/jetbrains.png"
alt="Jetbrains support" height="150" border="10" /></a>

/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.index.{BinarySearchIndex, HashIndex, SortedIndex}
import swaydb.data.IO
import swaydb.data.slice.Reader

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader): IO[Option[Persistent]] =
    for {
      footer <- SegmentFooter.read(reader)
      hashIndex <- footer.hashIndexOffset.map(offset => HashIndex.read(offset, reader).map(Some(_))).getOrElse(IO.none)
      binarySearchIndex <- footer.binarySearchIndexOffset.map(offset => BinarySearchIndex.read(offset, reader).map(Some(_))).getOrElse(IO.none)
      got <- get(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader.reset(),
        hashIndex = hashIndex,
        binarySearchIndex = binarySearchIndex,
        sortedIndexOffset = footer.sortedIndexOffset,
        hasRange = footer.hasRange
      )
    } yield got

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader): IO[Option[Persistent]] =
    SegmentFooter
      .read(reader)
      .flatMap(footer => lower(matcher, startFrom, reader, footer.sortedIndexOffset))

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader): IO[Option[Persistent]] =
    SegmentFooter
      .read(reader)
      .flatMap(footer => higher(matcher, startFrom, reader, footer.sortedIndexOffset))

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader,
            offset: SortedIndex.Offset): IO[Option[Persistent]] =
    SortedIndex.find(
      matcher = matcher,
      startFrom = startFrom,
      reader = reader,
      offset = offset
    )

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader,
             offset: SortedIndex.Offset): IO[Option[Persistent]] =
    SortedIndex.find(
      matcher = matcher,
      startFrom = startFrom,
      reader = reader,
      offset = offset
    )

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          hashIndex: Option[HashIndex],
          binarySearchIndex: Option[BinarySearchIndex],
          sortedIndexOffset: SortedIndex.Offset,
          hasRange: Boolean): IO[Option[Persistent]] =
    hashIndex map {
      hashIndex =>
        HashIndex.get(
          matcher = matcher.toNextPrefixCompressedMatcher,
          reader = reader,
          hashIndex = hashIndex,
          sortedIndexOffset = sortedIndexOffset
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)
          case None =>
            if (hashIndex.miss == 0 && !hasRange)
              IO.none
            else
              get(
                matcher = matcher,
                startFrom = startFrom,
                reader = reader,
                binarySearchIndex = binarySearchIndex,
                sortedIndexOffset = sortedIndexOffset
              )
        }
    } getOrElse {
      get(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        binarySearchIndex = binarySearchIndex,
        sortedIndexOffset = sortedIndexOffset
      )
    }

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          binarySearchIndex: Option[BinarySearchIndex],
          sortedIndexOffset: SortedIndex.Offset): IO[Option[Persistent]] =
    binarySearchIndex map {
      binarySearchIndex =>
        BinarySearchIndex.get(
          matcher = matcher,
          reader = reader,
          index = binarySearchIndex,
          sortedIndexOffset = sortedIndexOffset
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (binarySearchIndex.isFullBinarySearchIndex)
              IO.none
            else
              get(
                matcher = matcher,
                startFrom = startFrom,
                reader = reader,
                offset = sortedIndexOffset
              )
        }
    } getOrElse {
      get(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        offset = sortedIndexOffset
      )
    }

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          offset: SortedIndex.Offset): IO[Option[Persistent]] =
    SortedIndex.find(
      matcher = matcher,
      startFrom = startFrom,
      reader = reader,
      offset = offset
    )
}

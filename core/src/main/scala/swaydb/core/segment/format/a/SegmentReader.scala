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
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.SegmentException.SegmentCorruptionException
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.segment.format.a.index.{BinarySearchIndex, HashIndex, SortedIndex}
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.tailrec

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
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
    } yield {

      ???
    }

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    SegmentFooter.read(reader) flatMap (lower(matcher, startFrom, reader, _))

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    SegmentFooter.read(reader) flatMap (higher(matcher, startFrom, reader, _))

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            reader: Reader,
            footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      reader = reader,
  //      footer = footer
  //    )
    ???

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             reader: Reader,
             footer: SegmentFooter)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      reader = reader,
  //      footer = footer
  //    )
    ???

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          reader: Reader,
          hashIndex: Option[HashIndex],
          binarySearchIndex: Option[BinarySearchIndex],
          sortedIndexOffset: SortedIndex.Offset,
          hasRange: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
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
          sortedIndexOffset: SortedIndex.Offset)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
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
            SortedIndex.find(
              matcher = matcher,
              startFrom = startFrom,
              reader = reader,
              offset = sortedIndexOffset
            )
        }
    } getOrElse {
      SortedIndex.find(
        matcher = matcher,
        startFrom = startFrom,
        reader = reader,
        offset = sortedIndexOffset
      )
    }
}

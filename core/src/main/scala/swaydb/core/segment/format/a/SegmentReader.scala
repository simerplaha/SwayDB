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
import swaydb.core.io.reader.BlockReader
import swaydb.core.segment.format.a.block.{BinarySearchIndex, HashIndex, SortedIndex, Values}
import swaydb.data.IO

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentReader extends LazyLogging {

  def lower(matcher: KeyMatcher.Lower,
            startFrom: Option[Persistent],
            binarySearch: Option[BlockReader[BinarySearchIndex]],
            sortedIndex: BlockReader[SortedIndex],
            values: Option[BlockReader[Values]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      segmentReader = reader,
  //      index = index
  //    )
    ???

  def higher(matcher: KeyMatcher.Higher,
             startFrom: Option[Persistent],
             binarySearch: Option[BlockReader[BinarySearchIndex]],
             sortedIndex: BlockReader[SortedIndex],
             values: Option[BlockReader[Values]]): IO[Option[Persistent]] =
  //    SortedIndex.find(
  //      matcher = matcher,
  //      startFrom = startFrom,
  //      segmentReader = reader,
  //      index = index
  //    )
    ???

  def get(matcher: KeyMatcher.Get,
          startFrom: Option[Persistent],
          hashIndex: Option[BlockReader[HashIndex]],
          binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
          sortedIndex: BlockReader[SortedIndex],
          valuesReader: Option[BlockReader[Values]],
          hasRange: Boolean): IO[Option[Persistent]] =
    hashIndex map {
      hashIndex =>
        HashIndex.get(
          matcher = matcher.whilePrefixCompressed,
          hashIndex = hashIndex,
          sortedIndex = sortedIndex,
          values = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)
          case None =>
            if (hashIndex.block.miss == 0 && !hasRange)
              IO.none
            else
              get(
                matcher = matcher,
                start = startFrom,
                binarySearchIndex = binarySearchIndex,
                sortedIndex = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      get(
        matcher = matcher,
        start = startFrom,
        binarySearchIndex = binarySearchIndex,
        sortedIndex = sortedIndex,
        valuesReader = valuesReader
      )
    }

  private def get(matcher: KeyMatcher.Get,
                  start: Option[Persistent],
                  binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
                  sortedIndex: BlockReader[SortedIndex],
                  valuesReader: Option[BlockReader[Values]]): IO[Option[Persistent]] =
    binarySearchIndex map {
      binarySearchIndex =>
        BinarySearchIndex.get(
          matcher = matcher.whilePrefixCompressed,
          start = start,
          end = None,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          values = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (binarySearchIndex.block.isFullBinarySearchIndex)
              IO.none
            else
              SortedIndex.find(
                matcher = matcher,
                startFrom = start,
                indexReader = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndex.find(
        matcher = matcher,
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = valuesReader
      )
    }
}

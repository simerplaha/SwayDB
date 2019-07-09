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
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

/**
  * All public APIs are wrapped around a try catch block because eager fetches on IO's results (.get).
  * Eventually need to re-factor this code to use for comprehension.
  *
  * Leaving as it is now because it's easier to read.
  */
private[core] object SegmentSearcher extends LazyLogging {

  def lower(key: Slice[Byte],
            start: Option[Persistent],
            end: Option[Persistent],
            binarySearch: Option[BlockReader[BinarySearchIndex]],
            sortedIndex: BlockReader[SortedIndex],
            values: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearch map {
      binarySearchIndex =>
        BinarySearchIndex.search(
          matcher =
            if (sortedIndex.block.hasPrefixCompression)
              KeyMatcher.Lower.WhilePrefixCompressed(key)
            else
              KeyMatcher.Lower.MatchOnly(key),
          start = start,
          end = end,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          values = values
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (binarySearchIndex.block.isFullBinarySearchIndex)
              IO.none
            else
              SortedIndex.find(
                matcher = KeyMatcher.Lower(key),
                startFrom = start,
                indexReader = sortedIndex,
                valuesReader = values
              )
        }
    } getOrElse {
      SortedIndex.find(
        matcher = KeyMatcher.Lower(key),
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = values
      )
    }

  def higher(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             binarySearch: Option[BlockReader[BinarySearchIndex]],
             sortedIndex: BlockReader[SortedIndex],
             values: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    if (start.isEmpty)
      IO.none
    else
      SortedIndex.find(
        matcher = KeyMatcher.Higher.MatchOnly(key),
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = values
      )
  } flatMap {
    found =>
      if (found.isDefined)
        IO.Success(found)
      else
        binarySearch map {
          binarySearchIndex =>
            BinarySearchIndex.search(
              matcher =
                if (sortedIndex.block.hasPrefixCompression)
                  KeyMatcher.Higher.WhilePrefixCompressed(key)
                else
                  KeyMatcher.Higher.MatchOnly(key),
              start = start,
              end = end,
              binarySearchIndex = binarySearchIndex,
              sortedIndex = sortedIndex,
              values = values
            ) flatMap {
              case some @ Some(_) =>
                IO.Success(some)

              case None =>
                if (binarySearchIndex.block.isFullBinarySearchIndex)
                  IO.none
                else
                  SortedIndex.find(
                    matcher = KeyMatcher.Higher(key),
                    startFrom = start,
                    indexReader = sortedIndex,
                    valuesReader = values
                  )
            }
        } getOrElse {
          SortedIndex.find(
            matcher = KeyMatcher.Higher(key),
            startFrom = start,
            indexReader = sortedIndex,
            valuesReader = values
          )
        }
  }

  def get(key: Slice[Byte],
          start: Option[Persistent],
          end: Option[Persistent],
          hashIndex: Option[BlockReader[HashIndex]],
          binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
          sortedIndex: BlockReader[SortedIndex],
          valuesReader: Option[BlockReader[Values]],
          hasRange: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] = {
    hashIndex map {
      hashIndex =>
        HashIndex.get(
          matcher =
            if (sortedIndex.block.hasPrefixCompression)
              KeyMatcher.Get.WhilePrefixCompressed(key)
            else
              KeyMatcher.Get.MatchOnly(key),
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
                key = key,
                start = start,
                end = end,
                binarySearchIndex = binarySearchIndex,
                sortedIndex = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      get(
        key = key,
        start = start,
        end = end,
        binarySearchIndex = binarySearchIndex,
        sortedIndex = sortedIndex,
        valuesReader = valuesReader
      )
    }
  }

  private def get(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchIndex: Option[BlockReader[BinarySearchIndex]],
                  sortedIndex: BlockReader[SortedIndex],
                  valuesReader: Option[BlockReader[Values]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchIndex map {
      binarySearchIndex =>
        BinarySearchIndex.search(
          matcher =
            if (sortedIndex.block.hasPrefixCompression)
              KeyMatcher.Get.WhilePrefixCompressed(key)
            else
              KeyMatcher.Get.MatchOnly(key),
          start = start,
          end = end,
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
                matcher = KeyMatcher.Get(key),
                startFrom = start,
                indexReader = sortedIndex,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndex.find(
        matcher = KeyMatcher.Get(key),
        startFrom = start,
        indexReader = sortedIndex,
        valuesReader = valuesReader
      )
    }
}

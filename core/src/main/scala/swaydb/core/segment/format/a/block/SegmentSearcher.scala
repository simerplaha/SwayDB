/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.core.segment.format.a.block

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.reader.UnblockedReader
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

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchReader map {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.searchLower(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case SearchResult.Some(_, lower) =>
            if (binarySearchIndexReader.block.isFullIndex || end.exists(end => lower.nextIndexOffset == end.indexOffset))
              IO.Success(Some(lower))
            else
              SortedIndexBlock.searchLower(
                key = key,
                startFrom = Some(lower),
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )

          case SearchResult.None(lower) =>
            if (binarySearchIndexReader.block.isFullIndex) {
              IO.none
            } else {
              assert(lower.isEmpty, "Lower is non-empty")
              SortedIndexBlock.searchLower(
                key = key,
                startFrom = start,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
            }
        }
    } getOrElse {
      SortedIndexBlock.searchLower(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearchReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    start map {
      start =>
        SortedIndexBlock.searchHigherSeekOne(
          key = key,
          startFrom = start,
          indexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          found =>
            if (found.isDefined)
              IO.Success(found)
            else
              binarySearchHigher(
                key = key,
                start = Some(start),
                end = end,
                binarySearchReader = binarySearchReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      binarySearchHigher(
        key = key,
        start = start,
        end = end,
        binarySearchReader = binarySearchReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def binarySearchHigher(key: Slice[Byte],
                         start: Option[Persistent],
                         end: Option[Persistent],
                         binarySearchReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                         sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                         valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchReader map {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.searchHigher(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          result =>
            if (binarySearchIndexReader.block.isFullIndex)
              IO.Success(result.toOption)
            else
              result match {
                case SearchResult.None(lower) =>
                  SortedIndexBlock.searchHigher(
                    key = key,
                    startFrom = lower orElse start,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )

                case SearchResult.Some(lower, value) =>
                  if (lower.exists(_.nextIndexOffset == value.indexOffset))
                    IO.Success(result.toOption)
                  else
                    SortedIndexBlock.searchHigher(
                      key = key,
                      startFrom = lower orElse start,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader
                    )
              }
        }
    } getOrElse {
      SortedIndexBlock.searchHigher(
        key = key,
        startFrom = start,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             hashIndexReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
             binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReaderReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
             hasRange: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    hashIndexReader map {
      hashIndexReader =>
        HashIndexBlock.search(
          key = key,
          hashIndexReader = hashIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReaderReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (hashIndexReader.block.isPerfect && !hasRange)
              IO.none
            else
              search(
                key = key,
                start = start,
                end = end,
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReaderReader
              )
        }
    } getOrElse {
      search(
        key = key,
        start = start,
        end = end,
        binarySearchIndexReader = binarySearchIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReaderReader
      )
    }

  private def search(key: Slice[Byte],
                     start: Option[Persistent],
                     end: Option[Persistent],
                     binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                     sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Option[Persistent]] =
    binarySearchIndexReader map {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.search(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case SearchResult.Some(_, value) =>
            IO.Success(Some(value))

          case SearchResult.None(lower) =>
            if (binarySearchIndexReader.block.isFullIndex)
              IO.none
            else
              SortedIndexBlock.search(
                key = key,
                startFrom = lower orElse start,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndexBlock.search(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }
}

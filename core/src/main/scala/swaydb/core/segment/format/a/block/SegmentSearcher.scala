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
import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.data.io.Core
import swaydb.Error.Segment.ErrorHandler
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] object SegmentSearcher extends LazyLogging {

  def search(key: Slice[Byte],
             start: Option[Persistent],
             end: Option[Persistent],
             hashIndexReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
             binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
             hasRange: Boolean,
             hashIndexSearchOnly: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] =
    hashIndexReader map {
      hashIndexReader =>
        HashIndexBlock.search(
          key = key,
          hashIndexReader = hashIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case some @ Some(_) =>
            IO.Success(some)

          case None =>
            if (hashIndexSearchOnly || (hashIndexReader.block.isPerfect && !hasRange))
              IO.none
            else
              search(
                key = key,
                start = start,
                end = end,
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      if (hashIndexSearchOnly)
        IO.none
      else
        search(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        )
    }

  private def search(key: Slice[Byte],
                     start: Option[Persistent],
                     end: Option[Persistent],
                     binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                     sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                     valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] =
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

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: Option[Persistent],
                   binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] =
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
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      binarySearchHigher(
        key = key,
        start = start,
        end = end,
        binarySearchIndexReader = binarySearchIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def assertLowerAndStart(start: Option[Persistent], lower: Option[Persistent])(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit =
    if (start.isDefined)
      if (lower.isEmpty || keyOrder.lt(lower.get.key, start.get.key))
        throw new Exception(s"Lower ${lower.map(_.key.readInt())} is not greater than or equal to start ${start.map(_.key.readInt())}")
      else
        ()

  private def binarySearchHigher(key: Slice[Byte],
                                 start: Option[Persistent],
                                 end: Option[Persistent],
                                 binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                                 sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                 valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] =
    binarySearchIndexReader map {
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
                  assertLowerAndStart(start, lower)
                  SortedIndexBlock.searchHigher(
                    key = key,
                    startFrom = lower orElse start,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )

                case SearchResult.Some(lower, value) =>
                  assertLowerAndStart(start, lower)

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

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent]] =
    binarySearchIndexReader map {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.searchLower(
          key = key,
          start = start,
          end = end,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case SearchResult.Some(lowerLower, lower) =>
            assert(lowerLower.isEmpty, "lowerLower is not empty")
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
            assert(lower.isEmpty, "Lower is non-empty")
            if (binarySearchIndexReader.block.isFullIndex)
              IO.none
            else
              SortedIndexBlock.searchLower(
                key = key,
                startFrom = start,
                indexReader = sortedIndexReader,
                valuesReader = valuesReader
              )
        }
    } getOrElse {
      SortedIndexBlock.searchLower(
        key = key,
        startFrom = start,
        indexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }
}

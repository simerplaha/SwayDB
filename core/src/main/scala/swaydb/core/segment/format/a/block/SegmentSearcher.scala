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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.SegmentReadThreadState
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Options._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] object SegmentSearcher extends LazyLogging {

  def search(key: Slice[Byte],
             start: Option[Persistent.Partial],
             end: => Option[Persistent.Partial],
             hashIndexReader: => IO[swaydb.Error.Segment, Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]],
             binarySearchIndexReader: IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
             hasRange: Boolean,
             keyValueCount: => IO[swaydb.Error.Segment, Int],
             threadState: SegmentReadThreadState)(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    when(start.isDefined && threadState.isSequentialRead())(start) map {
      startFrom =>
        SortedIndexBlock.searchSeekOne(
          key = key,
          start = startFrom,
          indexReader = sortedIndexReader,
          fullRead = true,
          valuesReader = valuesReader
        ) flatMap {
          found =>
            if (found.isDefined) {
              threadState.notifySuccessfulSequentialRead()
              IO.Right(found)
            } else {
              hashIndexSearch(
                key = key,
                start = start,
                end = end,
                keyValueCount = keyValueCount,
                hashIndexReader = hashIndexReader,
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReader,
                hasRange = hasRange
              )
            }
        }
    } getOrElse {
      hashIndexSearch(
        key = key,
        start = start,
        end = end,
        keyValueCount = keyValueCount,
        hashIndexReader = hashIndexReader,
        binarySearchIndexReader = binarySearchIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader,
        hasRange = hasRange
      )
    }

  def hashIndexSearch(key: Slice[Byte],
                      start: Option[Persistent.Partial],
                      end: => Option[Persistent.Partial],
                      hashIndexReader: => IO[swaydb.Error.Segment, Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]],
                      binarySearchIndexReader: IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
                      sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                      hasRange: Boolean,
                      keyValueCount: => IO[swaydb.Error.Segment, Int])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    hashIndexReader flatMap {
      hashIndexReader =>
        hashIndexReader map {
          hashIndexReader =>
            HashIndexBlock.search(
              key = key,
              hashIndexReader = hashIndexReader,
              sortedIndexReader = sortedIndexReader,
              valuesReader = valuesReader
            ) flatMap {
              case some @ Some(_) =>
                IO.Right(some)

              case None =>
                if (hashIndexReader.block.isPerfect && !hasRange)
                  IO.none
                else
                  binarySearch(
                    key = key,
                    start = start,
                    end = end,
                    keyValueCount = keyValueCount,
                    binarySearchIndexReader = binarySearchIndexReader,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader
                  )
            }
        } getOrElse {
          binarySearch(
            key = key,
            start = start,
            end = end,
            keyValueCount = keyValueCount,
            binarySearchIndexReader = binarySearchIndexReader,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )
        }
    }

  private def binarySearch(key: Slice[Byte],
                           start: Option[Persistent.Partial],
                           end: => Option[Persistent.Partial],
                           keyValueCount: => IO[swaydb.Error.Segment, Int],
                           binarySearchIndexReader: IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
                           sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                           valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    binarySearchIndexReader flatMap {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.search(
          key = key,
          lowest = start,
          highest = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case BinaryGet.Some(_, value) =>
            IO.Right(Some(value))

          case BinaryGet.None(_) =>
            IO.none
        }
    }

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent.Partial],
                   end: => Option[Persistent.Partial],
                   keyValueCount: => IO[swaydb.Error.Segment, Int],
                   binarySearchIndexReader: => IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    start map {
      start =>
        SortedIndexBlock.searchHigherMatchOnly(
          key = key,
          startFrom = start,
          sortedIndexReader = sortedIndexReader,
          fullRead = false,
          valuesReader = valuesReader
        ) flatMap {
          found =>
            if (found.isDefined)
              IO.Right(found)
            else
              binarySearchHigher(
                key = key,
                start = Some(start),
                end = end,
                keyValueCount = keyValueCount,
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
        keyValueCount = keyValueCount,
        binarySearchIndexReader = binarySearchIndexReader,
        sortedIndexReader = sortedIndexReader,
        valuesReader = valuesReader
      )
    }

  def assertLowerAndStart(start: Option[Persistent.Partial], lower: Option[Persistent.Partial])(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit =
    if (start.isDefined && lower.nonEmpty)
      if (lower.isEmpty || keyOrder.lt(lower.get.key, start.get.key))
        throw new Exception(s"Lower ${lower.map(_.key.readInt())} is not greater than or equal to start ${start.map(_.key.readInt())}")
      else
        ()

  private def binarySearchHigher(key: Slice[Byte],
                                 start: Option[Persistent.Partial],
                                 end: => Option[Persistent.Partial],
                                 keyValueCount: => IO[swaydb.Error.Segment, Int],
                                 binarySearchIndexReader: => IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
                                 sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                                 valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    binarySearchIndexReader flatMap {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.searchHigher(
          key = key,
          start = start,
          end = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case BinaryGet.None(_) =>
            IO.none

          case BinaryGet.Some(_, value) =>
            IO.Right(Some(value))
        }
    }

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent.Partial],
                  end: => Option[Persistent.Partial],
                  keyValueCount: => IO[swaydb.Error.Segment, Int],
                  binarySearchIndexReader: => IO[swaydb.Error.Segment, Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Segment, Option[Persistent.Partial]] =
    binarySearchIndexReader flatMap {
      binarySearchIndexReader =>
        BinarySearchIndexBlock.searchLower(
          key = key,
          start = start,
          end = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) flatMap {
          case BinaryGet.None(_) =>
            IO.none

          case BinaryGet.Some(_, value) =>
            IO.Right(Some(value))
        }
    }
}

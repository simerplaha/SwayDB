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

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Persistent
import swaydb.core.segment.ReadState
import swaydb.core.segment.format.a.block.binarysearch.{BinarySearchGetResult, BinarySearchIndexBlock}
import swaydb.core.segment.format.a.block.hashindex.{HashIndexBlock, HashIndexSearchResult}
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.MinMax
import swaydb.core.util.Options._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] object SegmentSearcher extends LazyLogging {

  //  var seqSeeks = 0
  //  var successfulSeqSeeks = 0
  //  var failedSeqSeeks = 0
  //
  //  var hashIndexSeeks = 0
  //  var successfulHashIndexSeeks = 0
  //  var failedHashIndexSeeks = 0

  def search(path: Path,
             key: Slice[Byte],
             start: Option[Persistent],
             end: => Option[Persistent],
             hashIndexReader: => Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
             binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
             hasRange: Boolean,
             keyValueCount: => Int,
             readState: ReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   partialKeyOrder: KeyOrder[Persistent.Partial]): Option[Persistent] =
    when(start.isDefined && readState.isSequential(path))(start) match {
      case Some(startFrom) =>
        //        seqSeeks += 1
        val found =
          SortedIndexBlock.searchSeekOne(
            key = key,
            start = startFrom,
            indexReader = sortedIndexReader,
            valuesReader = valuesReader
          )

        if (found.isDefined) {
          //          successfulSeqSeeks += 1
          found
        } else {
          //          failedSeqSeeks += 1
          val result =
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

          val isSequential = result.exists(_.indexOffset == startFrom.nextIndexOffset)

          readState.setSequential(
            path = path,
            isSequential = isSequential
          )

          result
        }

      case None =>
        val result =
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

        val isSequential =
          result exists {
            result =>
              start.exists(_.nextIndexOffset == result.indexOffset)
          }

        readState.setSequential(
          path = path,
          isSequential = isSequential
        )

        result
    }

  def hashIndexSearch(key: Slice[Byte],
                      start: Option[Persistent],
                      end: => Option[Persistent],
                      hashIndexReader: => Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                      binarySearchIndexReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                      sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                      hasRange: Boolean,
                      keyValueCount: => Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                             partialKeyOrder: KeyOrder[Persistent.Partial]): Option[Persistent] =
    hashIndexReader match {
      case Some(hashIndexReader) =>
        //        hashIndexSeeks += 1
        //println
        //println(s"Search key: ${key.readInt()}")
        HashIndexBlock.search(
          key = key,
          hashIndexReader = hashIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ) match {
          case none: HashIndexSearchResult.None =>
            if (hashIndexReader.block.isPerfect && !sortedIndexReader.block.hasPrefixCompression && !hasRange) {
              None
            } else {
              //              failedHashIndexSeeks += 1

              val lowest =
                if (none.lower.isEmpty || start.isEmpty)
                  start orElse none.lower
                else
                  MinMax.maxFavourLeft(start, none.lower)

              val highest =
                if (none.higher.isEmpty || end.isEmpty)
                  none.higher orElse end
                else
                  MinMax.minFavourLeft(end, none.higher)

              BinarySearchIndexBlock.search(
                key = key,
                lowest = lowest.map(_.toPersistent),
                highest = highest.map(_.toPersistent),
                keyValuesCount = keyValueCount,
                binarySearchIndexReader = binarySearchIndexReader,
                sortedIndexReader = sortedIndexReader,
                valuesReader = valuesReader
              ).toOptionApply(_.toPersistent)
            }

          case HashIndexSearchResult.Some(keyValue) =>
            //            successfulHashIndexSeeks += 1
            Some(keyValue.toPersistent)
        }

      case None =>
        BinarySearchIndexBlock.search(
          key = key,
          lowest = start,
          highest = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ).toOptionApply(_.toPersistent)
    }

  def searchHigher(key: Slice[Byte],
                   start: Option[Persistent],
                   end: => Option[Persistent],
                   keyValueCount: => Int,
                   binarySearchIndexReader: => Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                           partialKeyOrder: KeyOrder[Persistent.Partial]): Option[Persistent] =
    start match {
      case Some(startFrom) =>
        val found =
          SortedIndexBlock.searchHigherMatchOnly(
            key = key,
            startFrom = startFrom,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          )

        if (found.isDefined)
          found
        else
          BinarySearchIndexBlock.searchHigher(
            key = key,
            start = start,
            end = end,
            keyValuesCount = keyValueCount,
            binarySearchIndexReader = binarySearchIndexReader,
            sortedIndexReader = sortedIndexReader,
            valuesReader = valuesReader
          ).map(_.toPersistent)

      case None =>
        BinarySearchIndexBlock.searchHigher(
          key = key,
          start = start,
          end = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReader = binarySearchIndexReader,
          sortedIndexReader = sortedIndexReader,
          valuesReader = valuesReader
        ).map(_.toPersistent)
    }

  def searchLower(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  keyValueCount: => Int,
                  binarySearchIndexReader: => Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                          partialOrdering: KeyOrder[Persistent.Partial]): Option[Persistent] =
    BinarySearchIndexBlock.searchLower(
      key = key,
      start = start,
      end = end,
      keyValuesCount = keyValueCount,
      binarySearchIndexReader = binarySearchIndexReader,
      sortedIndexReader = sortedIndexReader,
      valuesReader = valuesReader
    ).map(_.toPersistent)
}

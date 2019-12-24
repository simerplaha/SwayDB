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
import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.segment.ReadState
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Options._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.SomeOrNone._

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
             start: PersistentOptional,
             end: => PersistentOptional,
             hashIndexReaderNullable: => UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
             binarySearchIndexReaderNullable: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
             sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
             valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
             hasRange: Boolean,
             keyValueCount: => Int,
             readState: ReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOptional =
    when[PersistentOptional](start.isSomeS && readState.isSequential(path), Persistent.Null)(start) match {
      case startFrom: Persistent =>
        //        seqSeeks += 1
        val found =
          SortedIndexBlock.searchSeekOne(
            key = key,
            start = startFrom,
            indexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable
          )

        if (found.isSomeS) {
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
              hashIndexReaderNullable = hashIndexReaderNullable,
              binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
              sortedIndexReader = sortedIndexReader,
              valuesReaderNullable = valuesReaderNullable,
              hasRange = hasRange
            )

          val isSequential = result.existsS(_.indexOffset == startFrom.nextIndexOffset)

          readState.setSequential(
            path = path,
            isSequential = isSequential
          )

          result
        }

      case Persistent.Null =>
        val result =
          hashIndexSearch(
            key = key,
            start = start,
            end = end,
            keyValueCount = keyValueCount,
            hashIndexReaderNullable = hashIndexReaderNullable,
            binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
            sortedIndexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable,
            hasRange = hasRange
          )

        val isSequential =
          result existsS {
            result =>
              start.existsS(_.nextIndexOffset == result.indexOffset)
          }

        readState.setSequential(
          path = path,
          isSequential = isSequential
        )

        result
    }

  def hashIndexSearch(key: Slice[Byte],
                      start: PersistentOptional,
                      end: => PersistentOptional,
                      hashIndexReaderNullable: => UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                      binarySearchIndexReaderNullable: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
                      hasRange: Boolean,
                      keyValueCount: => Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                             partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOptional = {
    val hashIndex = hashIndexReaderNullable

    if (hashIndex == null)
      BinarySearchIndexBlock.search(
        key = key,
        lowest = start,
        highest = end,
        keyValuesCount = keyValueCount,
        binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      ).toPersistentOptional
    else {
      //        hashIndexSeeks += 1
      //println
      //println(s"Search key: ${key.readInt()}")
      HashIndexBlock.search(
        key = key,
        hashIndexReader = hashIndex,
        sortedIndexReader = sortedIndexReader,
        valuesReaderNullable = valuesReaderNullable
      ) match {
        case Persistent.Partial.Null =>
          if (hashIndex.block.isPerfect && !sortedIndexReader.block.hasPrefixCompression && !hasRange) {
            Persistent.Null
          } else {
            //              failedHashIndexSeeks += 1
            BinarySearchIndexBlock.search(
              key = key,
              lowest = start,
              highest = end,
              keyValuesCount = keyValueCount,
              binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
              sortedIndexReader = sortedIndexReader,
              valuesReaderNullable = valuesReaderNullable
            ).toPersistentOptional
          }

        case keyValue: Persistent.Partial =>
          //            successfulHashIndexSeeks += 1
          keyValue.toPersistent
      }
    }
  }

  def searchHigher(key: Slice[Byte],
                   start: PersistentOptional,
                   end: => PersistentOptional,
                   keyValueCount: => Int,
                   binarySearchIndexReaderNullable: => UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                           partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOptional =
    start match {
      case startFrom: Persistent =>
        val found =
          SortedIndexBlock.searchHigherMatchOnly(
            key = key,
            startFrom = startFrom,
            sortedIndexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable
          )

        if (found.isSomeS)
          found
        else
          BinarySearchIndexBlock.searchHigher(
            key = key,
            start = start,
            end = end,
            keyValuesCount = keyValueCount,
            binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
            sortedIndexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable
          )

      case Persistent.Null =>
        BinarySearchIndexBlock.searchHigher(
          key = key,
          start = start,
          end = end,
          keyValuesCount = keyValueCount,
          binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
          sortedIndexReader = sortedIndexReader,
          valuesReaderNullable = valuesReaderNullable
        )
    }

  def searchLower(key: Slice[Byte],
                  start: PersistentOptional,
                  end: PersistentOptional,
                  keyValueCount: => Int,
                  binarySearchIndexReaderNullable: => UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                  valuesReaderNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                          partialOrdering: KeyOrder[Persistent.Partial]): PersistentOptional =
    BinarySearchIndexBlock.searchLower(
      key = key,
      start = start,
      end = end,
      keyValuesCount = keyValueCount,
      binarySearchIndexReaderNullable = binarySearchIndexReaderNullable,
      sortedIndexReader = sortedIndexReader,
      valuesReaderNullable = valuesReaderNullable
    )
}

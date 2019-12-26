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
import swaydb.core
import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.segment.ReadState
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
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

  /**
   * Sets read state after successful sequential read.
   */
  private def setReadStateOnSuccessSequentialRead(path: Path,
                                                  readState: ReadState,
                                                  segmentStateOrNull: ReadState.SegmentState,
                                                  found: Persistent): Unit =
    if (segmentStateOrNull == null) {
      val segmentState =
        new ReadState.SegmentState(
          nextIndexOffset = found.nextIndexOffset,
          nextKeySizeOrNull = found.nextKeySize,
          isSequential = true
        )

      readState.setSegmentState(path, segmentState)
    } else {
      //mutate segmentState for next sequential read
      segmentStateOrNull.nextIndexOffset = found.nextIndexOffset
      segmentStateOrNull.nextKeySizeOrNull = found.nextKeySize
      segmentStateOrNull.isSequential = true
    }

  /**
   * Sets read state after a random read WITHOUT an existing [[ReadState.SegmentState]] exists.
   */
  private def setReadStateAfterRandomRead(path: Path,
                                          start: PersistentOptional,
                                          readState: ReadState,
                                          found: PersistentOptional): Unit =

    if (found.isSomeS) {
      val foundKeyValue = found.getS

      val segmentState =
        new core.segment.ReadState.SegmentState(
          nextIndexOffset = foundKeyValue.nextIndexOffset,
          nextKeySizeOrNull = foundKeyValue.nextKeySize,
          isSequential = start.isSomeS && foundKeyValue.indexOffset == start.getS.nextIndexOffset
        )

      readState.setSegmentState(path, segmentState)
    }

  /**
   * Sets read state after a random read WITH an existing [[ReadState.SegmentState]] exists.
   */
  private def mutateReadStateAfterRandomRead(path: Path,
                                             readState: ReadState,
                                             segmentState: ReadState.SegmentState, //should not be null.
                                             found: PersistentOptional): Unit =
    if (found.isSomeS) {
      val foundKeyValue = found.getS
      segmentState.isSequential = foundKeyValue.indexOffset == segmentState.nextIndexOffset
      segmentState.nextIndexOffset = foundKeyValue.nextIndexOffset
      segmentState.nextKeySizeOrNull = foundKeyValue.nextKeySize
    } else {
      segmentState.isSequential = false
    }

  /**
   * Sets read state after a random read where [[ReadState.SegmentState]] can be null.
   */
  private def setReadStateAfterRandomReadCheckNull(path: Path,
                                                   start: PersistentOptional,
                                                   readState: ReadState,
                                                   segmentStateOrNull: ReadState.SegmentState,
                                                   found: PersistentOptional): Unit =
    if (segmentStateOrNull == null)
      setReadStateAfterRandomRead(
        path = path,
        start = start,
        readState = readState,
        found = found
      )
    else
      mutateReadStateAfterRandomRead(
        path = path,
        readState = readState,
        segmentState = segmentStateOrNull,
        found = found
      )

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
                                   partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOptional = {
    val segmentStateOrNull = readState getSegmentStateOrNull path
    if (segmentStateOrNull == null || segmentStateOrNull.isSequential) {
      //        seqSeeks += 1
      val found =
        if (segmentStateOrNull == null)
          SortedIndexBlock.searchSeekOne(
            key = key,
            fromPosition = 0,
            keySizeOrNull = null,
            indexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable
          )
        else
          SortedIndexBlock.searchSeekOne(
            key = key,
            fromPosition = segmentStateOrNull.nextIndexOffset,
            keySizeOrNull = segmentStateOrNull.nextKeySizeOrNull,
            indexReader = sortedIndexReader,
            valuesReaderNullable = valuesReaderNullable
          )

      if (found.isSomeS) { //found is sequential read.
        setReadStateOnSuccessSequentialRead(
          path = path,
          readState = readState,
          segmentStateOrNull = segmentStateOrNull,
          found = found.getS
        )

        found
      } else { //not found via sequential seek.
        //          failedSeqSeeks += 1
        val found =
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

        setReadStateAfterRandomReadCheckNull(
          path = path,
          start = start,
          readState = readState,
          segmentStateOrNull = segmentStateOrNull,
          found = found
        )

        found
      }

    } else {
      val found =
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

      //segmentStateOrNull is never null here. If it's null it's a sequential read.
      mutateReadStateAfterRandomRead(
        path = path,
        readState = readState,
        segmentState = segmentStateOrNull,
        found = found
      )

      found
    }
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

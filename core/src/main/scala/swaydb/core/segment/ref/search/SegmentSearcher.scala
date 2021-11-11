/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package swaydb.core.segment.ref.search

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, PersistentOption}
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] trait SegmentSearcher {

  def searchSequential(key: Slice[Byte],
                       start: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                             partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption

  def searchRandom(key: Slice[Byte],
                   start: PersistentOption,
                   end: => PersistentOption,
                   hashIndexReaderOrNull: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                   binarySearchIndexReaderOrNull: => UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
                   hasRange: Boolean,
                   keyValueCount: => Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption

  def searchHigherRandomly(key: Slice[Byte],
                           start: PersistentOption,
                           end: PersistentOption,
                           keyValueCount: => Int,
                           binarySearchIndexReaderOrNull: => UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                           sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                 partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption

  def searchHigherSequentially(key: Slice[Byte],
                               start: PersistentOption,
                               sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                               valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                     partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption

  def searchLower(key: Slice[Byte],
                  start: PersistentOption,
                  end: PersistentOption,
                  keyValueCount: Int,
                  binarySearchIndexReaderOrNull: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                  valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                        partialOrdering: KeyOrder[Persistent.Partial]): PersistentOption
}

private[core] object SegmentSearcher extends SegmentSearcher with LazyLogging {
  //
  //  var seqSeeks = 0
  //  var successfulSeqSeeks = 0
  //  var failedSeqSeeks = 0
  //
  //  var hashIndexSeeks = 0
  //  var successfulHashIndexSeeks = 0
  //  var failedHashIndexSeeks = 0

  def searchSequential(key: Slice[Byte],
                       start: PersistentOption,
                       sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                       valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                             partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption =
    if (start.isSomeS)
      SortedIndexBlock.searchSeekOne(
        key = key,
        start = start.getS,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    else
      SortedIndexBlock.searchSeekOne(
        key = key,
        fromPosition = 0,
        keySizeOrZero = 0,
        indexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

  def searchRandom(key: Slice[Byte],
                   start: PersistentOption,
                   end: => PersistentOption,
                   hashIndexReaderOrNull: UnblockedReader[HashIndexBlockOffset, HashIndexBlock],
                   binarySearchIndexReaderOrNull: => UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                   sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                   valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock],
                   hasRange: Boolean,
                   keyValueCount: => Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                          partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption =
    if (hashIndexReaderOrNull == null)
      BinarySearchIndexBlock.search(
        key = key,
        lowest = start,
        highest = end,
        keyValuesCount = keyValueCount,
        binarySearchIndexReaderOrNull = binarySearchIndexReaderOrNull,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      ).toPersistentOptional
    else
    //      hashIndexSeeks += 1
    //println
    //println(s"Search key: ${key.readInt()}")
      HashIndexBlock.search(
        key = key,
        hashIndexReader = hashIndexReaderOrNull,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      ) match {
        case Persistent.Partial.Null =>
          if (hashIndexReaderOrNull.block.isPerfect && !sortedIndexReader.block.hasPrefixCompression && !hasRange) {
            Persistent.Null
          } else {
            //            failedHashIndexSeeks += 1
            BinarySearchIndexBlock.search(
              key = key,
              lowest = start,
              highest = end,
              keyValuesCount = keyValueCount,
              binarySearchIndexReaderOrNull = binarySearchIndexReaderOrNull,
              sortedIndexReader = sortedIndexReader,
              valuesReaderOrNull = valuesReaderOrNull
            ).toPersistentOptional
          }

        case keyValue: Persistent.Partial =>
          //          successfulHashIndexSeeks += 1
          keyValue.toPersistent
      }

  def searchHigherSequentially(key: Slice[Byte],
                               start: PersistentOption,
                               sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                               valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                     partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption =
    if (start.isSomeS)
      if (keyOrder.equiv(key, start.getS.key))
        SortedIndexBlock.readNextKeyValue(
          previous = start.getS,
          sortedIndexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
      else
        SortedIndexBlock.searchHigherSeekOne(
          key = key,
          startFrom = start.getS,
          sortedIndexReader = sortedIndexReader,
          valuesReaderOrNull = valuesReaderOrNull
        )
    else
      SortedIndexBlock.searchHigherSeekOne(
        key = key,
        fromPosition = 0,
        keySizeOrZero = 0,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

  def searchHigherRandomly(key: Slice[Byte],
                           start: PersistentOption,
                           end: PersistentOption,
                           keyValueCount: => Int,
                           binarySearchIndexReaderOrNull: => UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                           sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                           valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                 partialKeyOrder: KeyOrder[Persistent.Partial]): PersistentOption =
    BinarySearchIndexBlock.searchHigher(
      key = key,
      start = start,
      end = end,
      keyValuesCount = keyValueCount,
      binarySearchIndexReaderOrNull = binarySearchIndexReaderOrNull,
      sortedIndexReader = sortedIndexReader,
      valuesReaderOrNull = valuesReaderOrNull
    )

  def searchLower(key: Slice[Byte],
                  start: PersistentOption,
                  end: PersistentOption,
                  keyValueCount: Int,
                  binarySearchIndexReaderOrNull: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                  sortedIndexReader: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
                  valuesReaderOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                        partialOrdering: KeyOrder[Persistent.Partial]): PersistentOption =
    if (sortedIndexReader.block.optimiseForReverseIteration && !sortedIndexReader.block.hasPrefixCompression && end.isSomeS && keyOrder.equiv(key, end.getS.key))
      SortedIndexBlock.read(
        fromOffset = end.getS.previousIndexOffset,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
    else
      BinarySearchIndexBlock.searchLower(
        key = key,
        start = start,
        end = end,
        keyValuesCount = keyValueCount,
        binarySearchIndexReaderOrNull = binarySearchIndexReaderOrNull,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )
}

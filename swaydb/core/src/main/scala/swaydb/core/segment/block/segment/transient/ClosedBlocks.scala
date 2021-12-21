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

package swaydb.core.segment.block.segment.transient

import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset, BinarySearchIndexBlockStateOption}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset, BloomFilterBlockStateOption}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset, HashIndexBlockStateOption}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset, SortedIndexBlockState}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset, ValuesBlockStateOption}
import swaydb.core.util.MinMax
import swaydb.slice.Slice

import scala.concurrent.duration.Deadline

private[block] class ClosedBlocks(val sortedIndex: SortedIndexBlockState,
                                  val values: ValuesBlockStateOption,
                                  val hashIndex: HashIndexBlockStateOption,
                                  val binarySearchIndex: BinarySearchIndexBlockStateOption,
                                  val bloomFilter: BloomFilterBlockStateOption,
                                  val minMaxFunction: Option[MinMax[Slice[Byte]]],
                                  prepareForCachingSegmentBlocksOnCreate: Boolean) {
  def nearestDeadline: Option[Deadline] = sortedIndex.nearestDeadline

  val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      Some(SortedIndexBlock.unblockedReader(sortedIndex))
    else
      None

  val valuesUnblockedReader: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      values.mapS(ValuesBlock.unblockedReader)
    else
      None

  val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      hashIndex.mapS(HashIndexBlock.unblockedReader)
    else
      None

  val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      binarySearchIndex.mapS(BinarySearchIndexBlock.unblockedReader)
    else
      None

  val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      bloomFilter.mapS(BloomFilterBlock.unblockedReader)
    else
      None
}

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

import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset, BinarySearchIndexBlockState}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset, BloomFilterBlockState}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset, HashIndexBlockState}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset, SortedIndexBlockState}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset, ValuesBlockState}
import swaydb.core.util.MinMax
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[block] class ClosedBlocks(val sortedIndex: SortedIndexBlockState,
                                  val values: Option[ValuesBlockState],
                                  val hashIndex: Option[HashIndexBlockState],
                                  val binarySearchIndex: Option[BinarySearchIndexBlockState],
                                  val bloomFilter: Option[BloomFilterBlockState],
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
      values.map(ValuesBlock.unblockedReader)
    else
      None

  val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      hashIndex.map(HashIndexBlock.unblockedReader)
    else
      None

  val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      binarySearchIndex.map(BinarySearchIndexBlock.unblockedReader)
    else
      None

  val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      bloomFilter.map(BloomFilterBlock.unblockedReader)
    else
      None
}

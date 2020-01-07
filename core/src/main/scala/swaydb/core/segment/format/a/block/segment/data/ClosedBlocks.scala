/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.segment.format.a.block.segment.data

import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.MinMax
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[block] class ClosedBlocks(val sortedIndex: SortedIndexBlock.State,
                                  val values: Option[ValuesBlock.State],
                                  val hashIndex: Option[HashIndexBlock.State],
                                  val binarySearchIndex: Option[BinarySearchIndexBlock.State],
                                  val bloomFilter: Option[BloomFilterBlock.State],
                                  val minMaxFunction: Option[MinMax[Slice[Byte]]],
                                  prepareForCachingSegmentBlocksOnCreate: Boolean) {
  def nearestDeadline: Option[Deadline] = sortedIndex.nearestDeadline

  val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      Some(SortedIndexBlock.unblockedReader(sortedIndex))
    else
      None

  val valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      values.map(ValuesBlock.unblockedReader)
    else
      None

  val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      hashIndex.map(HashIndexBlock.unblockedReader)
    else
      None

  val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      binarySearchIndex.map(BinarySearchIndexBlock.unblockedReader)
    else
      None

  val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
    if (prepareForCachingSegmentBlocksOnCreate)
      bloomFilter.map(BloomFilterBlock.unblockedReader)
    else
      None
}

/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.segment.data

import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Sliced

import scala.concurrent.duration.Deadline

class ClosedBlocksWithFooter(val minKey: Sliced[Byte],
                             val maxKey: MaxKey[Sliced[Byte]],
                             val functionMinMax: Option[MinMax[Sliced[Byte]]],
                             val nearestDeadline: Option[Deadline],
                             //values
                             val valuesBlockHeader: Option[Sliced[Byte]],
                             val valuesBlock: Option[Sliced[Byte]],
                             val valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                             //sortedIndex
                             val sortedIndexClosedState: SortedIndexBlock.State,
                             val sortedIndexBlockHeader: Sliced[Byte],
                             val sortedIndexBlock: Sliced[Byte],
                             val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                             //hashIndex
                             val hashIndexBlockHeader: Option[Sliced[Byte]],
                             val hashIndexBlock: Option[Sliced[Byte]],
                             val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                             //binarySearch
                             val binarySearchIndexBlockHeader: Option[Sliced[Byte]],
                             val binarySearchIndexBlock: Option[Sliced[Byte]],
                             val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                             //bloomFilter
                             val bloomFilterBlockHeader: Option[Sliced[Byte]],
                             val bloomFilterBlock: Option[Sliced[Byte]],
                             val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                             //footer
                             val footerBlock: Sliced[Byte]) {

  val segmentHeader: Sliced[Byte] = Slice.create[Byte](Byte.MaxValue)

  val segmentBytes: Sliced[Sliced[Byte]] = {
    val allBytes = Slice.create[Sliced[Byte]](13)
    allBytes add segmentHeader

    valuesBlockHeader foreach (allBytes add _)
    valuesBlock foreach (allBytes add _)

    allBytes add sortedIndexBlockHeader
    allBytes add sortedIndexBlock

    hashIndexBlockHeader foreach (allBytes add _)
    hashIndexBlock foreach (allBytes add _)

    binarySearchIndexBlockHeader foreach (allBytes add _)
    binarySearchIndexBlock foreach (allBytes add _)

    bloomFilterBlockHeader foreach (allBytes add _)
    bloomFilterBlock foreach (allBytes add _)

    allBytes add footerBlock
  }

  //If sortedIndexUnblockedReader is defined then caching is enabled
  //so read footer block.

  val footerUnblocked: Option[SegmentFooterBlock] =
    if (sortedIndexUnblockedReader.isDefined)
      Some(
        SegmentFooterBlock.readCRCPassed(
          footerStartOffset = 0,
          footerSize = footerBlock.size,
          footerBytes = footerBlock
        )
      )
    else
      None

  def isEmpty: Boolean =
    segmentBytes.exists(_.isEmpty)

  def segmentSize =
    segmentBytes.foldLeft(0)(_ + _.size)

  def flattenSegmentBytes: Sliced[Byte] = {
    val size = segmentBytes.foldLeft(0)(_ + _.size)
    val slice = Slice.create[Byte](size)
    segmentBytes foreach (slice addAll _)
    assert(slice.isFull)
    slice
  }
}

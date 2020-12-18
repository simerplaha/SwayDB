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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.block.segment.data

import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

class TransientSegmentRef(val minKey: Slice[Byte],
                          val maxKey: MaxKey[Slice[Byte]],
                          val functionMinMax: Option[MinMax[Slice[Byte]]],
                          val nearestDeadline: Option[Deadline],
                          //counts
                          val updateCount: Int,
                          val rangeCount: Int,
                          val putCount: Int,
                          val putDeadlineCount: Int,
                          val keyValueCount: Int,
                          //values
                          val valuesBlockHeader: Option[Slice[Byte]],
                          val valuesBlock: Option[Slice[Byte]],
                          val valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                          //sortedIndex
                          val sortedIndexClosedState: SortedIndexBlock.State,
                          val sortedIndexBlockHeader: Slice[Byte],
                          val sortedIndexBlock: Slice[Byte],
                          val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                          //hashIndex
                          val hashIndexBlockHeader: Option[Slice[Byte]],
                          val hashIndexBlock: Option[Slice[Byte]],
                          val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                          //binarySearch
                          val binarySearchIndexBlockHeader: Option[Slice[Byte]],
                          val binarySearchIndexBlock: Option[Slice[Byte]],
                          val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                          //bloomFilter
                          val bloomFilterBlockHeader: Option[Slice[Byte]],
                          val bloomFilterBlock: Option[Slice[Byte]],
                          val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                          //footer
                          val footerBlock: Slice[Byte]) {

  val segmentHeader: Slice[Byte] = Slice.of[Byte](Byte.MaxValue)

  val segmentBytes: Slice[Slice[Byte]] = {
    val allBytes = Slice.of[Slice[Byte]](13)
    allBytes add segmentHeader

    valuesBlockHeader foreach allBytes.add
    valuesBlock foreach allBytes.add

    allBytes add sortedIndexBlockHeader
    allBytes add sortedIndexBlock

    hashIndexBlockHeader foreach allBytes.add
    hashIndexBlock foreach allBytes.add

    binarySearchIndexBlockHeader foreach allBytes.add
    binarySearchIndexBlock foreach allBytes.add

    bloomFilterBlockHeader foreach allBytes.add
    bloomFilterBlock foreach allBytes.add

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

  def flattenSegmentBytes: Slice[Byte] = {
    val size = segmentBytes.foldLeft(0)(_ + _.size)
    val slice = Slice.of[Byte](size)
    segmentBytes foreach (slice addAll _)
    assert(slice.isFull)
    slice
  }
}

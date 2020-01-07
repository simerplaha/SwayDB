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

package swaydb.core.segment.format.a.block.segment

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

import scala.concurrent.duration.Deadline

object TransientSegment {

  def apply(ref: TransientSegmentBlock): TransientSegment =
    new TransientSegment(
      minKey = ref.minKey,
      maxKey = ref.maxKey,
      sortedIndexClosedState = ref.sortedIndexClosedState,
      segmentBytes = ref.segmentBytes,
      minMaxFunctionId = ref.functionMinMax,
      nearestDeadline = ref.nearestDeadline,
      valuesUnblockedReader = ref.valuesUnblockedReader,
      sortedIndexUnblockedReader = ref.sortedIndexUnblockedReader,
      hashIndexUnblockedReader = ref.hashIndexUnblockedReader,
      binarySearchUnblockedReader = ref.binarySearchUnblockedReader,
      bloomFilterUnblockedReader = ref.bloomFilterUnblockedReader,
      footerUnblocked = ref.footerUnblocked
    )
}

class TransientSegment(val minKey: Slice[Byte],
                       val maxKey: MaxKey[Slice[Byte]],
                       val segmentBytes: Slice[Slice[Byte]],
                       val minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                       val nearestDeadline: Option[Deadline],
                       val valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                       val sortedIndexClosedState: SortedIndexBlock.State,
                       val sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                       val hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                       val binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                       val bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                       val footerUnblocked: Option[SegmentFooterBlock]) {

  def isEmpty: Boolean =
    segmentBytes.exists(_.isEmpty)

  def segmentSize =
    segmentBytes.foldLeft(0)(_ + _.size)

  def flattenSegmentBytes: Slice[Byte] = {
    val size = segmentBytes.foldLeft(0)(_ + _.size)
    val slice = Slice.create[Byte](size)
    segmentBytes foreach (slice addAll _)
    assert(slice.isFull)
    slice
  }

  def flattenSegment: (Slice[Byte], Option[Deadline]) =
    (flattenSegmentBytes, nearestDeadline)

  override def toString: String =
    s"TransientSegment Segment. Size: ${segmentSize}"
}
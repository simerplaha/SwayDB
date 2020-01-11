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

import swaydb.core.data.Memory
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

sealed trait TransientSegment {
  def isEmpty: Boolean
  def minKey: Slice[Byte]
  def maxKey: MaxKey[Slice[Byte]]
  def segmentSize: Int
  def segmentBytes: Slice[Slice[Byte]]
  def minMaxFunctionId: Option[MinMax[Slice[Byte]]]
  def nearestDeadline: Option[Deadline]
  def flattenSegmentBytes: Slice[Byte]
  def flattenSegment: (Slice[Byte], Option[Deadline])
}

object TransientSegment {

  case class One(minKey: Slice[Byte],
                 maxKey: MaxKey[Slice[Byte]],
                 segmentBytes: Slice[Slice[Byte]],
                 minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                 nearestDeadline: Option[Deadline],
                 valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                 sortedIndexClosedState: SortedIndexBlock.State,
                 sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                 hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                 binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                 bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                 footerUnblocked: Option[SegmentFooterBlock]) extends TransientSegment {

    override def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    override def segmentSize =
      segmentBytes.foldLeft(0)(_ + _.size)

    override def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }

    override def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestDeadline)

    override def toString: String =
      s"TransientSegment Segment. Size: ${segmentSize}"

    def toKeyValue(offset: Int, size: Int): Slice[Memory] =
      TransientSegmentSerialiser.toKeyValue(
        one = this,
        offset = offset,
        size = size
      )
  }

  case class Many(minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]],
                  minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                  nearestDeadline: Option[Deadline],
                  segments: Slice[TransientSegment.One],
                  segmentBytes: Slice[Slice[Byte]]) extends TransientSegment {

    override def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    override def segmentSize: Int =
      segmentBytes.foldLeft(0)(_ + _.size)

    override def flattenSegmentBytes: Slice[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }

    override def flattenSegment: (Slice[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestDeadline)

    override def toString: String =
      s"TransientSegment Segment. Size: ${segmentSize}"
  }
}

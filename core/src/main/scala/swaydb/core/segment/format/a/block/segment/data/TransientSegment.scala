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
import swaydb.data.slice.Slice.Sliced

import scala.concurrent.duration.Deadline

sealed trait TransientSegment {
  def isEmpty: Boolean
  def minKey: Sliced[Byte]
  def maxKey: MaxKey[Sliced[Byte]]
  def segmentSize: Int
  def segmentBytes: Sliced[Sliced[Byte]]
  def minMaxFunctionId: Option[MinMax[Sliced[Byte]]]
  def nearestPutDeadline: Option[Deadline]
  def flattenSegmentBytes: Sliced[Byte]
  def flattenSegment: (Sliced[Byte], Option[Deadline])
}

object TransientSegment {

  case class One(minKey: Sliced[Byte],
                 maxKey: MaxKey[Sliced[Byte]],
                 segmentBytes: Sliced[Sliced[Byte]],
                 minMaxFunctionId: Option[MinMax[Sliced[Byte]]],
                 nearestPutDeadline: Option[Deadline],
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

    override def flattenSegmentBytes: Sliced[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }

    override def flattenSegment: (Sliced[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestPutDeadline)

    override def toString: String =
      s"TransientSegment Segment. Size: ${segmentSize}"

    def toKeyValue(offset: Int, size: Int): Sliced[Memory] =
      TransientSegmentSerialiser.toKeyValue(
        one = this,
        offset = offset,
        size = size
      )
  }

  case class Many(minKey: Sliced[Byte],
                  maxKey: MaxKey[Sliced[Byte]],
                  headerSize: Int,
                  minMaxFunctionId: Option[MinMax[Sliced[Byte]]],
                  nearestPutDeadline: Option[Deadline],
                  segments: Sliced[TransientSegment.One],
                  segmentBytes: Sliced[Sliced[Byte]]) extends TransientSegment {

    override def isEmpty: Boolean =
      segmentBytes.exists(_.isEmpty)

    override def segmentSize: Int =
      segmentBytes.foldLeft(0)(_ + _.size)

    override def flattenSegmentBytes: Sliced[Byte] = {
      val size = segmentBytes.foldLeft(0)(_ + _.size)
      val slice = Slice.create[Byte](size)
      segmentBytes foreach (slice addAll _)
      assert(slice.isFull)
      slice
    }

    override def flattenSegment: (Sliced[Byte], Option[Deadline]) =
      (flattenSegmentBytes, nearestPutDeadline)

    override def toString: String =
      s"TransientSegment Segment. Size: $segmentSize"
  }
}

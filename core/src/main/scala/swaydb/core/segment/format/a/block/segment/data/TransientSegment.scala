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

package swaydb.core.segment.format.a.block.segment.data

import swaydb.core.data.Memory
import swaydb.core.segment.SegmentRef
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
  def minKey: Slice[Byte]
  def maxKey: MaxKey[Slice[Byte]]
  def hasEmptyByteSlice: Boolean
  def nearestPutDeadline: Option[Deadline]
  def segmentSize: Int
}

object TransientSegment {

  sealed trait Singleton extends TransientSegment {
    def copyWithFileHeader(headerBytes: Slice[Byte]): Singleton

    def hasEmptyByteSliceIgnoreHeader: Boolean
    def segmentSizeIgnoreHeader: Int

    def valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]
    def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]]
    def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]
    def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]
    def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]]
    def footerUnblocked: Option[SegmentFooterBlock]

    def toKeyValue(offset: Int, size: Int): Slice[Memory] =
      TransientSegmentSerialiser.toKeyValue(
        singleton = this,
        offset = offset,
        size = size
      )
  }

  case class Remote(fileHeader: Slice[Byte], segmentRef: SegmentRef) extends Singleton {
    override def minKey: Slice[Byte] =
      segmentRef.minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      segmentRef.maxKey

    override def nearestPutDeadline: Option[Deadline] =
      segmentRef.nearestPutDeadline

    override def hasEmptyByteSlice: Boolean =
      fileHeader.isEmpty || hasEmptyByteSliceIgnoreHeader

    override def hasEmptyByteSliceIgnoreHeader: Boolean =
      segmentRef.segmentSize == 0

    override def segmentSize: Int =
      fileHeader.size + segmentRef.segmentSize

    override def segmentSizeIgnoreHeader: Int =
      segmentRef.segmentSize

    override def valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
      segmentRef.segmentBlockCache.cachedValuesSliceReader()

    override def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]] =
      segmentRef.segmentBlockCache.cachedSortedIndexSliceReader()

    override def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]] =
      segmentRef.segmentBlockCache.cachedHashIndexSliceReader()

    override def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]] =
      segmentRef.segmentBlockCache.cachedBinarySearchIndexSliceReader()

    override def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
      segmentRef.segmentBlockCache.cachedBloomFilterSliceReader()

    override def footerUnblocked: Option[SegmentFooterBlock] =
      segmentRef.segmentBlockCache.cachedFooter()

    override def toString: String =
      s"TransientSegment.${this.productPrefix}. Size: ${segmentRef.segmentSize}.bytes"

    override def copyWithFileHeader(fileHeader: Slice[Byte]): Remote =
      copy(fileHeader = fileHeader)

  }

  case class One(minKey: Slice[Byte],
                 maxKey: MaxKey[Slice[Byte]],
                 fileHeader: Slice[Byte],
                 bodyBytes: Slice[Slice[Byte]],
                 minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                 nearestPutDeadline: Option[Deadline],
                 valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                 sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
                 hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
                 binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
                 bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                 footerUnblocked: Option[SegmentFooterBlock]) extends Singleton {

    def hasEmptyByteSlice: Boolean =
      fileHeader.isEmpty || hasEmptyByteSliceIgnoreHeader

    override def hasEmptyByteSliceIgnoreHeader: Boolean =
      bodyBytes.isEmpty || bodyBytes.exists(_.isEmpty)

    def segmentSize =
      bodyBytes.foldLeft(fileHeader.size)(_ + _.size)

    def segmentSizeIgnoreHeader =
      bodyBytes.foldLeft(0)(_ + _.size)

    override def copyWithFileHeader(fileHeader: Slice[Byte]): One =
      copy(fileHeader = fileHeader)

    override def toString: String =
      s"TransientSegment.${this.productPrefix}. Size: $segmentSize.bytes"

  }

  case class Many(minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]],
                  fileHeader: Slice[Byte],
                  minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                  nearestPutDeadline: Option[Deadline],
                  listSegment: TransientSegment.One,
                  segments: Slice[TransientSegment.Singleton]) extends TransientSegment {

    def hasEmptyByteSlice: Boolean =
      fileHeader.isEmpty || listSegment.hasEmptyByteSliceIgnoreHeader || segments.exists(_.hasEmptyByteSliceIgnoreHeader)

    def segmentSize =
      fileHeader.size + listSegment.segmentSizeIgnoreHeader + segments.foldLeft(0)(_ + _.segmentSizeIgnoreHeader)

    override def toString: String =
      s"TransientSegment.${this.productPrefix}. Size: $segmentSize.bytes"
  }
}

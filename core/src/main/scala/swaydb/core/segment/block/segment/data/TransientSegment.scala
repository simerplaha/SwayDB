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

import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.merge.MergeStats
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.{MemorySegment, Segment}
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline

sealed trait TransientSegment {
  def minKey: Slice[Byte]
  def maxKey: MaxKey[Slice[Byte]]
  def hasEmptyByteSlice: Boolean
  def nearestPutDeadline: Option[Deadline]
  def minMaxFunctionId: Option[MinMax[Slice[Byte]]]
  def segmentSize: Int
}

object TransientSegment {

  sealed trait Persistent extends TransientSegment

  /**
   * Points to a remote Segment this could be a [[SegmentRef]] or [[Segment]].
   */
  sealed trait Singleton extends Persistent

  /**
   * [[Fragment]] type is used by [[swaydb.core.segment.defrag.Defrag]] to
   * create a barrier between two groups of Segments.
   *
   * For example: if a Segment gets assigned to head and tail gaps but no mergeable
   * mid key-values then head and tail gaps cannot be joined.
   */
  sealed trait Fragment

  sealed trait Remote extends Singleton with Fragment {
    def iterator(): Iterator[KeyValue]
  }

  /**
   * This type does not extend any [[TransientSegment.Persistent]] types because it is only
   * used to creates a barrier between groups of key-values/Segments or SegmentRefs when
   * running [[swaydb.core.segment.defrag.Defrag]].
   */
  case object Fence extends Fragment

  case class Stats(stats: MergeStats.Persistent.Builder[swaydb.core.data.Memory, ListBuffer]) extends Fragment

  sealed trait OneOrRemoteRefOrMany extends Persistent

  sealed trait OneOrRemoteRef extends OneOrRemoteRefOrMany with Singleton {
    def copyWithFileHeader(headerBytes: Slice[Byte]): OneOrRemoteRef

    def hasEmptyByteSliceIgnoreHeader: Boolean
    def segmentSizeIgnoreHeader: Int

    def valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]
    def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]]
    def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]]
    def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]]
    def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]]
    def footerUnblocked: Option[SegmentFooterBlock]

    def toKeyValue(offset: Int, size: Int): Slice[swaydb.core.data.Memory] =
      TransientSegmentSerialiser.toKeyValue(
        singleton = this,
        offset = offset,
        size = size
      )
  }

  case class RemoteRef(fileHeader: Slice[Byte],
                       ref: SegmentRef) extends OneOrRemoteRef with Remote {
    override def minKey: Slice[Byte] =
      ref.minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      ref.maxKey

    override def nearestPutDeadline: Option[Deadline] =
      ref.nearestPutDeadline

    override def minMaxFunctionId: Option[MinMax[Slice[Byte]]] =
      ref.minMaxFunctionId

    override def hasEmptyByteSlice: Boolean =
      fileHeader.isEmpty || hasEmptyByteSliceIgnoreHeader

    override def hasEmptyByteSliceIgnoreHeader: Boolean =
      ref.segmentSize == 0

    override def segmentSize: Int =
      fileHeader.size + ref.segmentSize

    override def segmentSizeIgnoreHeader: Int =
      ref.segmentSize

    override def valuesUnblockedReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]] =
      ref.segmentBlockCache.cachedValuesSliceReader()

    override def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]] =
      ref.segmentBlockCache.cachedSortedIndexSliceReader()

    override def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]] =
      ref.segmentBlockCache.cachedHashIndexSliceReader()

    override def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]] =
      ref.segmentBlockCache.cachedBinarySearchIndexSliceReader()

    override def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]] =
      ref.segmentBlockCache.cachedBloomFilterSliceReader()

    override def footerUnblocked: Option[SegmentFooterBlock] =
      ref.segmentBlockCache.cachedFooter()

    override def iterator(): Iterator[KeyValue] =
      ref.iterator()

    override def copyWithFileHeader(fileHeader: Slice[Byte]): RemoteRef =
      copy(fileHeader = fileHeader)

  }

  case class RemoteSegment(segment: Segment,
                           removeDeletes: Boolean,
                           createdInLevel: Int,
                           valuesConfig: ValuesBlock.Config,
                           sortedIndexConfig: SortedIndexBlock.Config,
                           binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                           hashIndexConfig: HashIndexBlock.Config,
                           bloomFilterConfig: BloomFilterBlock.Config,
                           segmentConfig: SegmentBlock.Config) extends Remote {
    override def minKey: Slice[Byte] =
      segment.minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      segment.maxKey

    override def nearestPutDeadline: Option[Deadline] =
      segment.nearestPutDeadline

    override def minMaxFunctionId: Option[MinMax[Slice[Byte]]] =
      segment.minMaxFunctionId

    override def hasEmptyByteSlice: Boolean =
      false

    override def segmentSize: Int =
      segment.segmentSize

    override def iterator(): Iterator[KeyValue] =
      segment.iterator()
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
                 footerUnblocked: Option[SegmentFooterBlock]) extends OneOrRemoteRef with OneOrRemoteRefOrMany {

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
  }

  case class Many(minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]],
                  fileHeader: Slice[Byte],
                  minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                  nearestPutDeadline: Option[Deadline],
                  listSegment: TransientSegment.One,
                  segments: Slice[TransientSegment.OneOrRemoteRef]) extends OneOrRemoteRefOrMany {

    def hasEmptyByteSlice: Boolean =
      fileHeader.isEmpty || listSegment.hasEmptyByteSliceIgnoreHeader || segments.exists(_.hasEmptyByteSliceIgnoreHeader)

    def segmentSize =
      fileHeader.size + listSegment.segmentSizeIgnoreHeader + segments.foldLeft(0)(_ + _.segmentSizeIgnoreHeader)
  }


  case class Memory(segment: MemorySegment) extends TransientSegment {
    override def minKey: Slice[Byte] =
      segment.minKey

    override def maxKey: MaxKey[Slice[Byte]] =
      segment.maxKey

    override def nearestPutDeadline: Option[Deadline] =
      segment.nearestPutDeadline

    override def minMaxFunctionId: Option[MinMax[Slice[Byte]]] =
      segment.minMaxFunctionId

    override def hasEmptyByteSlice: Boolean =
      false

    override def segmentSize: Int =
      segment.segmentSize
  }
}

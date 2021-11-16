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

import swaydb.core.data.KeyValue
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset}
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockOffset}
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.{MemorySegment, PersistentSegment, Segment}
import swaydb.core.util.MinMax
import swaydb.data.MaxKey
import swaydb.slice.Slice

import scala.concurrent.Future
import scala.concurrent.duration.Deadline

sealed trait TransientSegment

object TransientSegment {

  case class Memory(segment: MemorySegment) extends TransientSegment

  sealed trait Persistent extends TransientSegment {
    def minKey: Slice[Byte]
    def maxKey: MaxKey[Slice[Byte]]
    def hasEmptyByteSlice: Boolean
    def nearestPutDeadline: Option[Deadline]
    def minMaxFunctionId: Option[MinMax[Slice[Byte]]]
    def updateCount: Int
    def rangeCount: Int
    def putCount: Int
    def keyValueCount: Int
    def putDeadlineCount: Int
    def segmentSize: Int
    def createdInLevel: Int
  }

  sealed trait SingletonOrFence

  /**
   * Points to a remote Segment this could be a [[SegmentRef]] or [[Segment]].
   */
  sealed trait Singleton extends Persistent with SingletonOrFence

  /**
   * [[Fragment]] type is used by [[swaydb.core.segment.defrag.Defrag]] to
   * create a barrier between two groups of Segments.
   *
   * For example: if a Segment gets assigned to head and tail gaps but no mergeable
   * mid key-values then head and tail gaps cannot be joined.
   */
  sealed trait Fragment[+S]

  sealed trait RemoteRefOrStats[+S]

  sealed trait Remote extends Singleton with Fragment[Nothing] {
    def iterator(inOneSeek: Boolean): Iterator[KeyValue]
  }

  /**
   * [[Fence]] is not a subtype of [[TransientSegment.Persistent]] because it is only
   * used to creates a barrier between groups of key-values/Segments or SegmentRefs when
   * running [[swaydb.core.segment.defrag.Defrag]].
   */
  case object Fence extends Fragment[Nothing] with SingletonOrFence {
    val slice = Slice(TransientSegment.Fence)
    val futureSuccessful = Future.successful(slice)
  }

  case class Stats[S](stats: S) extends Fragment[S] with RemoteRefOrStats[S]

  sealed trait OneOrRemoteRefOrMany extends Persistent

  sealed trait OneOrRemoteRef extends OneOrRemoteRefOrMany with Singleton {
    def copyWithFileHeader(headerBytes: Slice[Byte]): OneOrRemoteRef

    def hasEmptyByteSliceIgnoreHeader: Boolean
    def segmentSizeIgnoreHeader: Int

    def valuesUnblockedReader: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]]
    def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]]
    def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]]
    def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]]
    def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]]
    def footerUnblocked: Option[SegmentFooterBlock]

    def toKeyValue(offset: Int, size: Int): Slice[swaydb.core.data.Memory] =
      TransientSegmentSerialiser.toKeyValue(
        singleton = this,
        offset = offset,
        size = size
      )
  }

  case object RemoteRef {
    @inline def apply(ref: SegmentRef): TransientSegment.RemoteRef =
      new RemoteRef(
        fileHeader = Slice.emptyBytes,
        ref = ref
      )
  }

  /**
   * Should not allow setting [[fileHeader]] at the time of creation because [[SegmentRef]]s are
   * mostly written when embedded within another [[Segment]].
   *
   * But it is a possibility that compaction can submit a large a [[SegmentRef]] to be written
   * as an independent [[Segment]] only then the [[fileHeader]] should be updated
   * via [[copyWithFileHeader]].
   */
  class RemoteRef private(val fileHeader: Slice[Byte],
                          val ref: SegmentRef) extends OneOrRemoteRef with Remote with RemoteRefOrStats[Nothing] {

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

    override def valuesUnblockedReader: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]] =
      ref.segmentBlockCache.cachedValuesSliceReader()

    override def sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]] =
      ref.segmentBlockCache.cachedSortedIndexSliceReader()

    override def hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]] =
      ref.segmentBlockCache.cachedHashIndexSliceReader()

    override def binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]] =
      ref.segmentBlockCache.cachedBinarySearchIndexSliceReader()

    override def bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]] =
      ref.segmentBlockCache.cachedBloomFilterSliceReader()

    override def footerUnblocked: Option[SegmentFooterBlock] =
      ref.segmentBlockCache.cachedFooter()

    override def iterator(inOneSeek: Boolean): Iterator[KeyValue] =
      ref.iterator(inOneSeek)

    override def copyWithFileHeader(fileHeader: Slice[Byte]): RemoteRef =
      new RemoteRef(fileHeader = fileHeader, ref = ref)

    override def updateCount: Int =
      ref.updateCount

    override def rangeCount: Int =
      ref.rangeCount

    override def putCount: Int =
      ref.putCount

    override def putDeadlineCount: Int =
      ref.putDeadlineCount

    override def keyValueCount: Int =
      ref.keyValueCount

    override def createdInLevel: Int =
      ref.createdInLevel
  }

  case class RemotePersistentSegment(segment: PersistentSegment) extends Remote {

    override def createdInLevel: Int =
      segment.createdInLevel

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

    override def iterator(inOneSeek: Boolean): Iterator[KeyValue] =
      segment.iterator(inOneSeek)

    override def updateCount: Int =
      segment.updateCount

    override def rangeCount: Int =
      segment.rangeCount

    override def putCount: Int =
      segment.putCount

    override def putDeadlineCount: Int =
      segment.putDeadlineCount

    override def keyValueCount: Int =
      segment.keyValueCount
  }

  case class One(minKey: Slice[Byte],
                 maxKey: MaxKey[Slice[Byte]],
                 fileHeader: Slice[Byte],
                 bodyBytes: Slice[Slice[Byte]],
                 minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                 nearestPutDeadline: Option[Deadline],
                 updateCount: Int,
                 rangeCount: Int,
                 putCount: Int,
                 putDeadlineCount: Int,
                 keyValueCount: Int,
                 createdInLevel: Int,
                 valuesUnblockedReader: Option[UnblockedReader[ValuesBlockOffset, ValuesBlock]],
                 sortedIndexUnblockedReader: Option[UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock]],
                 hashIndexUnblockedReader: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
                 binarySearchUnblockedReader: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
                 bloomFilterUnblockedReader: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
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

    override def toString: String =
      s"minKey: $minKey, maxKey: $maxKey, fileHeader: $fileHeader, segmentSize: $segmentSize"
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

    val (updateCount, rangeCount, putCount, putDeadlineCount, keyValueCount, createdInLevel) =
      segments.foldLeft((0, 0, 0, 0, 0, Int.MaxValue)) {
        case ((updateCount, rangeCount, putCount, putDeadlineCount, keyValueCount, createdInLevel), segment) =>
          (
            updateCount + segment.updateCount,
            rangeCount + segment.rangeCount,
            putCount + segment.putCount,
            putDeadlineCount + segment.putDeadlineCount,
            keyValueCount + segment.keyValueCount,
            createdInLevel min segment.createdInLevel
          )
      }

    override def toString: String =
      s"minKey: $minKey, maxKey: $maxKey, fileHeader: $fileHeader, segments: ${segments.size}"
  }
}

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

package swaydb.core.segment.ref

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Persistent, _}
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.block.hashindex.HashIndexBlock
import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache}
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.{SegmentSearcher, ThreadReadState}
import swaydb.core.util.MinMax
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListConcurrentLimit}
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.SomeOrNoneCovariant

import java.nio.file.Path
import scala.concurrent.duration._

private[core] sealed trait SegmentRefOption extends SomeOrNoneCovariant[SegmentRefOption, SegmentRef] {
  override def noneC: SegmentRefOption = SegmentRef.Null
}

private[core] case object SegmentRef extends LazyLogging {

  final case object Null extends SegmentRefOption {
    override def isNoneC: Boolean = true

    override def getC: SegmentRef = throw new Exception("SegmentRef is of type Null")
  }

  def apply(path: Path,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            nearestPutDeadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentReadIO,
            updateCount: Int,
            rangeCount: Int,
            putCount: Int,
            putDeadlineCount: Int,
            keyValueCount: Int,
            createdInLevel: Int,
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef = {
    val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent]] =
      keyValueMemorySweeper map {
        sweeper =>
          sweeper.maxKeyValuesPerSegment match {
            case Some(maxKeyValuesPerSegment) =>
              SkipListConcurrentLimit(
                limit = maxKeyValuesPerSegment,
                nullKey = Slice.Null,
                nullValue = Persistent.Null
              )

            case None =>
              SkipListConcurrent(
                nullKey = Slice.Null,
                nullValue = Persistent.Null
              )
          }
      }

    val segmentBlockCache =
      SegmentBlockCache(
        path = path,
        segmentIO = segmentIO,
        blockRef = blockRef,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )

    new SegmentRef(
      path = path,
      maxKey = maxKey,
      minKey = minKey,
      updateCount = updateCount,
      rangeCount = rangeCount,
      putCount = putCount,
      putDeadlineCount = putDeadlineCount,
      keyValueCount = keyValueCount,
      createdInLevel = createdInLevel,
      nearestPutDeadline = nearestPutDeadline,
      minMaxFunctionId = minMaxFunctionId,
      skipList = skipList,
      segmentBlockCache = segmentBlockCache
    )
  }
}

private[core] class SegmentRef(val path: Path,
                               val maxKey: MaxKey[Slice[Byte]],
                               val minKey: Slice[Byte],
                               val updateCount: Int,
                               val rangeCount: Int,
                               val putCount: Int,
                               val putDeadlineCount: Int,
                               val keyValueCount: Int,
                               val createdInLevel: Int,
                               val nearestPutDeadline: Option[Deadline],
                               val minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                               val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent]],
                               val segmentBlockCache: SegmentBlockCache)(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                         keyOrder: KeyOrder[Slice[Byte]]) extends SegmentRefOption with Assignable.Collection with LazyLogging {

  implicit val self: SegmentRef = this
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  override def key: Slice[Byte] =
    minKey

  override def isNoneC: Boolean =
    false

  override def getC: SegmentRef =
    this

  /**
   * Notes for why use putIfAbsent before adding to cache:
   *
   * Sometimes file seeks will be done if the last known cached key-value's ranges are smaller than the
   * key being searched. For example: Search key is 10, but the last lower cache key-value range is 1-5.
   * here it's unknown if a lower key 7 exists without doing a file seek. This is also one of the reasons
   * reverse iterations are slower than forward.
   */
  private[segment] def addToSkipList(keyValue: Persistent): Unit =
    skipList foreach {
      skipList =>
        //unslice not required anymore since SegmentSearch always unsliced.
        //keyValue.unsliceKeys
        if (skipList.putIfAbsent(keyValue.key, keyValue))
          keyValueMemorySweeper.foreach(_.add(keyValue, skipList))
    }

  private[segment] def applyToSkipList(f: SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent] => PersistentOption): PersistentOption =
    if (skipList.isDefined)
      f(skipList.get)
    else
      Persistent.Null

  def getFromCache(key: Slice[Byte]): PersistentOption =
    skipList match {
      case Some(skipList) =>
        skipList get key

      case None =>
        Persistent.Null
    }

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean = {
    val bloomFilterReader = segmentBlockCache.createBloomFilterReaderOrNull()
    bloomFilterReader == null ||
      BloomFilterBlock.mightContain(
        comparableKey = keyOrder.comparableKey(key),
        reader = bloomFilterReader
      )
  }

  def iterator(): Iterator[Persistent] =
    segmentBlockCache.iterator()

  def getFooter(): SegmentFooterBlock =
    segmentBlockCache.getFooter()

  def isKeyValueCacheEmpty =
    skipList.forall(_.isEmpty)

  def isBlockCacheEmpty =
    !segmentBlockCache.isCached

  def isFooterDefined: Boolean =
    segmentBlockCache.isFooterDefined

  def hasBloomFilter: Boolean =
    segmentBlockCache.getFooter().bloomFilterOffset.isDefined

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList.exists(_.contains(key))

  def cachedKeyValueSize: Int =
    skipList.foldLeft(0)(_ + _.size)

  def clearCachedKeyValues() =
    skipList.foreach(_.clear())

  def clearAllCaches() =
    segmentBlockCache.clear()

  def areAllCachesEmpty =
    isKeyValueCacheEmpty && !segmentBlockCache.isCached

  def readAllBytes(): Slice[Byte] =
    segmentBlockCache.readAllBytes()

  def segmentSize: Int =
    segmentBlockCache.segmentSize

  def hasUpdateOrRange: Boolean =
    updateCount > 0 || rangeCount > 0

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRefReader.get(key, threadState)

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRefReader.lower(key, threadState)

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRefReader.higher(key, threadState)

  def offset(): SegmentBlock.Offset =
    segmentBlockCache.offset()

  def blockCache(): Option[BlockCache.State] =
    segmentBlockCache.blockCache()

  override def equals(other: Any): Boolean =
    other match {
      case other: SegmentRef =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}

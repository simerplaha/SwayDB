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

package swaydb.core.segment

import java.nio.file.Path
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.FileSweeper
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.data.{Memory, _}
import swaydb.core.function.FunctionStore
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util._
import swaydb.core.util.skiplist.SkipListMap
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline
import scala.jdk.CollectionConverters._
import scala.collection.compat._

protected case class MemorySegment(path: Path,
                                   minKey: Slice[Byte],
                                   maxKey: MaxKey[Slice[Byte]],
                                   minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                   segmentSize: Int,
                                   hasRange: Boolean,
                                   hasPut: Boolean,
                                   createdInLevel: Int,
                                   private[segment] val skipList: SkipListMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                   nearestPutDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                                         functionStore: FunctionStore,
                                                                         fileSweeper: FileSweeperActor) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  override def formatId: Byte = 0

  override def put(newKeyValues: Slice[KeyValue],
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config,
                   pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val stats = MergeStats.memory[Memory, ListBuffer](ListBuffer.newBuilder)

      SegmentMerger.merge(
        newKeyValues = newKeyValues,
        oldKeyValuesCount = getKeyValueCount(),
        oldKeyValues = iterator(),
        stats = stats,
        isLastLevel = removeDeletes
      )

      if (stats.isEmpty)
        Slice.empty
      else
        Segment.memory(
          minSegmentSize = segmentConfig.minSize,
          maxKeyValueCountPerSegment = segmentConfig.maxCount,
          pathsDistributor = pathsDistributor,
          createdInLevel = createdInLevel,
          keyValues = stats.close
        )
    }

  override def refresh(removeDeletes: Boolean,
                       createdInLevel: Int,
                       valuesConfig: ValuesBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       segmentConfig: SegmentBlock.Config,
                       pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator): Slice[Segment] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val keyValues =
        Segment
          .toMemoryIterator(iterator(), removeDeletes)
          .to(Iterable)

      val mergeStats =
        new MergeStats.Memory.Closed[Iterable](
          isEmpty = false,
          keyValues = keyValues
        )

      Segment.memory(
        minSegmentSize = segmentConfig.minSize,
        maxKeyValueCountPerSegment = segmentConfig.maxCount,
        pathsDistributor = pathsDistributor,
        createdInLevel = createdInLevel,
        keyValues = mergeStats
      )
    }

  override def getFromCache(key: Slice[Byte]): MemoryOption =
    skipList.get(key)

  override def get(key: Slice[Byte], threadState: ThreadReadState): MemoryOption =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          Memory.Null

        case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
          Memory.Null

        case _ =>
          if (hasRange)
            skipList.floor(key) match {
              case range: Memory.Range if KeyValue.Range.contains(range, key) =>
                range

              case _ =>
                skipList.get(key)
            }
          else
            skipList.get(key)
      }

  def mightContainKey(key: Slice[Byte]): Boolean =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          false

        case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
          false

        case _ =>
          if (hasRange)
            skipList.floor(key) match {
              case range: Memory.Range if KeyValue.Range.contains(range, key) =>
                true

              case _ =>
                skipList.contains(key)
            }
          else
            skipList.contains(key)
      }

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId.exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  override def lower(key: Slice[Byte],
                     threadState: ThreadReadState): MemoryOption =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.lower(key)

  override def higher(key: Slice[Byte],
                      threadState: ThreadReadState): MemoryOption =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else if (hasRange)
      skipList.floor(key) match {
        case floorRange: Memory.Range if KeyValue.Range.contains(floorRange, key) =>
          floorRange

        case _ =>
          skipList.higher(key)
      }
    else
      skipList.higher(key)

  override def toSlice(): Slice[Memory] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val slice = Slice.newAggregator[Memory](skipList.size)
      skipList.values() forEach {
        new Consumer[Memory] {
          override def accept(value: Memory): Unit =
            slice add value
        }
      }
      slice.result
    }

  override def iterator(): Iterator[Memory] =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.values().iterator().asScala

  override def delete: Unit = {
    //cache should not be cleared.
    logger.trace(s"{}: DELETING FILE", path)
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      deleted = true
  }

  override val close: Unit =
    ()

  override def getKeyValueCount(): Int =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.size

  override def isOpen: Boolean =
    !deleted

  override def isFileDefined: Boolean =
    !deleted

  override def memory: Boolean =
    true

  override def persistent: Boolean =
    false

  override def existsOnDisk: Boolean =
    false

  override def isFooterDefined: Boolean =
    !deleted

  override def deleteSegmentsEventually: Unit =
    fileSweeper send FileSweeper.Command.Delete(this)

  override def clearCachedKeyValues(): Unit =
    ()

  override def clearAllCaches(): Unit =
    ()

  override def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList contains key

  override def isKeyValueCacheEmpty: Boolean =
    skipList.isEmpty

  def areAllCachesEmpty: Boolean =
    isKeyValueCacheEmpty

  override def cachedKeyValueSize: Int =
    skipList.size

  override def hasBloomFilter: Boolean =
    false

  override def isMMAP: Boolean =
    false
}

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

package swaydb.core.segment

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.compaction.CompactionConfig.CompactionParallelism
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.data._
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.defrag.DefragMemorySegment
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.skiplist.SkipListTreeMap
import swaydb.core.util._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{MaxKey, Slice, SliceOption}

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

sealed trait MemorySegmentOption {
  def asSegmentOption: SegmentOption
}

object MemorySegment {
  final case object Null extends MemorySegmentOption {
    override val asSegmentOption: SegmentOption =
      Segment.Null
  }
}

private[core] final case class MemorySegment(path: Path,
                                             minKey: Slice[Byte],
                                             maxKey: MaxKey[Slice[Byte]],
                                             minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                             segmentSize: Int,
                                             updateCount: Int,
                                             rangeCount: Int,
                                             putCount: Int,
                                             putDeadlineCount: Int,
                                             createdInLevel: Int,
                                             private[segment] val skipList: SkipListTreeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                             nearestPutDeadline: Option[Deadline],
                                             pathsDistributor: PathsDistributor)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                 timeOrder: TimeOrder[Slice[Byte]],
                                                                                 functionStore: FunctionStore,
                                                                                 fileSweeper: FileSweeper) extends Segment with MemorySegmentOption with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  override def formatId: Byte = 0

  override def asSegmentOption: SegmentOption =
    this

  def put(headGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
          tailGap: Iterable[Assignable.Gap[MergeStats.Memory.Builder[Memory, ListBuffer]]],
          newKeyValues: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                             executionContext: ExecutionContext,
                                             compactionParallelism: CompactionParallelism): Future[DefIO[MemorySegmentOption, Iterable[MemorySegment]]] =
    if (deleted)
      Future.failed(swaydb.Exception.NoSuchFile(path))
    else {
      implicit val segmentConfigImplicit: SegmentBlockConfig = segmentConfig

      DefragMemorySegment.runOnSegment(
        segment = this,
        nullSegment = MemorySegment.Null,
        headGap = headGap,
        tailGap = tailGap,
        newKeyValues = newKeyValues,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        pathsDistributor = pathsDistributor
      )
    }

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator): DefIO[MemorySegment, Slice[MemorySegment]] =
    if (deleted) {
      throw swaydb.Exception.NoSuchFile(path)
    } else {
      val mergeStats =
        new MergeStats.Memory.ClosedIgnoreStats[Iterator](
          isEmpty = false,
          keyValues = Segment.toMemoryIterator(iterator(segmentConfig.initialiseIteratorsInOneSeek), removeDeletes)
        )

      val refreshed =
        Segment.memory(
          minSegmentSize = segmentConfig.minSize,
          maxKeyValueCountPerSegment = segmentConfig.maxCount,
          pathsDistributor = pathsDistributor,
          createdInLevel = createdInLevel,
          stats = mergeStats
        )

      DefIO(this, refreshed)
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

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean =
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

  override def iterator(inOneSeek: Boolean): Iterator[Memory] =
    if (deleted)
      throw swaydb.Exception.NoSuchFile(path)
    else
      skipList.values().iterator

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

  override def keyValueCount: Int =
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

  override def existsOnDiskOrMemory: Boolean =
    !deleted

  override def isFooterDefined: Boolean =
    !deleted

  def delete(delay: FiniteDuration): Unit = {
    val deadline = delay.fromNow
    if (deadline.isOverdue())
      this.delete
    else
      fileSweeper send FileSweeper.Command.Delete(this, deadline)
  }

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

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
import swaydb.core.file.sweeper.{FileSweeper, FileSweeperCommand}
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
import swaydb.utils.{FiniteDurations, IDGenerator}

import java.nio.file.Path
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

private[core] sealed trait MemorySegmentOption {
  def asSegmentOption: SegmentOption
}

private[core] object MemorySegment {
  final case object Null extends MemorySegmentOption {
    override val asSegmentOption: SegmentOption =
      Segment.Null
  }

  def apply(minSegmentSize: Int,
            maxKeyValueCountPerSegment: Int,
            pathsDistributor: PathsDistributor,
            createdInLevel: Int,
            stats: MergeStats.Memory.ClosedIgnoreStats[IterableOnce])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                                      functionStore: CoreFunctionStore,
                                                                      fileSweeper: FileSweeper,
                                                                      idGenerator: IDGenerator): Slice[MemorySegment] =
    if (stats.isEmpty) {
      throw new Exception("Empty key-values submitted to memory Segment.")
    } else {
      val segments = ListBuffer.empty[MemorySegment]

      var skipList = SkipListTreeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
      var minMaxFunctionId: Option[MinMax[Slice[Byte]]] = None
      var nearestDeadline: Option[Deadline] = None
      var updateCount = 0
      var rangeCount = 0
      var putCount = 0
      var putDeadlineCount = 0
      var currentSegmentSize = 0
      var currentSegmentKeyValuesCount = 0
      var minKey: Slice[Byte] = null
      var lastKeyValue: Memory = null

      def setClosed(): Unit = {
        skipList = SkipListTreeMap[SliceOption[Byte], MemoryOption, Slice[Byte], Memory](Slice.Null, Memory.Null)(keyOrder)
        minMaxFunctionId = None
        nearestDeadline = None
        updateCount = 0
        rangeCount = 0
        putCount = 0
        putDeadlineCount = 0
        currentSegmentSize = 0
        currentSegmentKeyValuesCount = 0
        minKey = null
        lastKeyValue = null
      }

      def put(keyValue: Memory): Unit =
        keyValue.cut() match {
          case keyValue: Memory.Put =>
            putCount += 1
            if (keyValue.deadline.isDefined) putDeadlineCount += 1
            nearestDeadline = FiniteDurations.getNearestDeadline(nearestDeadline, keyValue.deadline)
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Update =>
            updateCount += 1
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Function =>
            updateCount += 1
            minMaxFunctionId = Some(MinMax.minMaxFunction(keyValue, minMaxFunctionId))
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.PendingApply =>
            updateCount += 1
            minMaxFunctionId = MinMax.minMaxFunction(keyValue.applies, minMaxFunctionId)
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Remove =>
            updateCount += 1
            skipList.put(keyValue.key, keyValue)

          case keyValue: Memory.Range =>
            rangeCount += 1

            keyValue.fromValue foreachS {
              case put: Value.Put =>
                putCount += 1
                if (put.deadline.isDefined) putDeadlineCount += 1
                nearestDeadline = FiniteDurations.getNearestDeadline(nearestDeadline, put.deadline)

              case _: Value.RangeValue =>
              //no need to do anything here. Just put deadline required.
            }
            minMaxFunctionId = MinMax.minMaxFunction(keyValue, minMaxFunctionId)
            skipList.put(keyValue.key, keyValue)
        }

      def createSegment() = {
        val path = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

        //Note: Memory key-values can be received from Persistent Segments in which case it's important that
        //all byte arrays are cutd before writing them to Memory Segment.

        val segment =
          MemorySegment(
            path = path,
            minKey = minKey.cut(),
            maxKey =
              lastKeyValue match {
                case range: Memory.Range =>
                  MaxKey.Range(range.fromKey.cut(), range.toKey.cut())

                case keyValue: Memory.Fixed =>
                  MaxKey.Fixed(keyValue.key.cut())
              },
            minMaxFunctionId = minMaxFunctionId,
            segmentSize = currentSegmentSize,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            createdInLevel = createdInLevel,
            skipList = skipList,
            nearestPutDeadline = nearestDeadline,
            pathsDistributor = pathsDistributor
          )

        segments += segment
        setClosed()
      }

      stats.keyValues foreach {
        keyValue =>
          if (minKey == null) minKey = keyValue.key
          lastKeyValue = keyValue

          put(keyValue)

          currentSegmentSize += MergeStats.Memory calculateSize keyValue
          currentSegmentKeyValuesCount += 1

          if (currentSegmentSize >= minSegmentSize || currentSegmentKeyValuesCount >= maxKeyValueCountPerSegment)
            createSegment()
      }

      if (lastKeyValue != null)
        createSegment()

      Slice.from(segments, segments.size)
    }

  def apply(keyValues: Iterator[KeyValue],
            pathsDistributor: PathsDistributor,
            removeDeletes: Boolean,
            minSegmentSize: Int,
            maxKeyValueCountPerSegment: Int,
            createdInLevel: Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                 timeOrder: TimeOrder[Slice[Byte]],
                                 functionStore: CoreFunctionStore,
                                 fileSweeper: FileSweeper,
                                 idGenerator: IDGenerator): Slice[MemorySegment] = {
    val builder =
      new MergeStats.Memory.ClosedIgnoreStats[Iterator](
        isEmpty = false,
        keyValues = Segment.toMemoryIterator(keyValues, removeDeletes)
      )

    MemorySegment(
      minSegmentSize = minSegmentSize,
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      stats = builder
    )
  }

  def copyFrom(segment: Segment,
               createdInLevel: Int,
               pathsDistributor: PathsDistributor,
               removeDeletes: Boolean,
               minSegmentSize: Int,
               maxKeyValueCountPerSegment: Int,
               initialiseIteratorsInOneSeek: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: CoreFunctionStore,
                                                      fileSweeper: FileSweeper,
                                                      idGenerator: IDGenerator): Slice[MemorySegment] =
    apply(
      keyValues = segment.iterator(initialiseIteratorsInOneSeek),
      pathsDistributor = pathsDistributor,
      removeDeletes = removeDeletes,
      minSegmentSize = minSegmentSize,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      createdInLevel = createdInLevel
    )


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
                                                                                 functionStore: CoreFunctionStore,
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
                                             executionContext: ExecutionContext): Future[DefIO[MemorySegmentOption, Iterable[MemorySegment]]] =
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
        MemorySegment(
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
        )(CoreFunctionStore.order)
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

  override def existsOnDisk(): Boolean =
    false

  override def existsOnDiskOrMemory(): Boolean =
    !deleted

  override def isFooterDefined: Boolean =
    !deleted

  def delete(delay: FiniteDuration): Unit = {
    val deadline = delay.fromNow
    if (deadline.isOverdue())
      this.delete()
    else
      fileSweeper send FileSweeperCommand.Delete(this, deadline)
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

  override def hasBloomFilter(): Boolean =
    false

  override def isMMAP: Boolean =
    false
}

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
import swaydb.utils.Aggregator
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.file.{DBFile, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.merge.KeyValueGrouper
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.{Assignable, Assigner}
import swaydb.core.segment.block.BlockCompressionInfo
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig}
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig}
import swaydb.core.segment.io.{SegmentReadIO, SegmentWritePersistentIO}
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, FileSweeperItem, MemorySweeper}
import swaydb.utils.Collections._
import swaydb.core.util._
import swaydb.core.skiplist.{SkipList, SkipListTreeMap}
import swaydb.slice.MaxKey
import swaydb.config.compaction.CompactionConfig.CompactionParallelism
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.SliceIOImplicits._
import swaydb.slice.{Slice, SliceOption}
import swaydb.effect.Effect
import swaydb.utils.Futures.FutureUnitImplicits
import swaydb.utils.{FiniteDurations, SomeOrNone}

import java.nio.file.Path
import scala.annotation.tailrec
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] sealed trait SegmentOption extends SomeOrNone[SegmentOption, Segment] {
  override def noneS: SegmentOption =
    Segment.Null
}

private[core] case object Segment extends LazyLogging {

  final case object Null extends SegmentOption {
    override def isNoneS: Boolean = true

    override def getS: Segment = throw new Exception("Segment is of type Null")
  }

  val emptyIterable = Iterable.empty[Segment]

  def memory(minSegmentSize: Int,
             maxKeyValueCountPerSegment: Int,
             pathsDistributor: PathsDistributor,
             createdInLevel: Int,
             stats: MergeStats.Memory.ClosedIgnoreStats[IterableOnce])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                                       functionStore: FunctionStore,
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

  def persistent(pathsDistributor: PathsDistributor,
                 createdInLevel: Int,
                 bloomFilterConfig: BloomFilterBlockConfig,
                 hashIndexConfig: HashIndexBlockConfig,
                 binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                 sortedIndexConfig: SortedIndexBlockConfig,
                 valuesConfig: ValuesBlockConfig,
                 segmentConfig: SegmentBlockConfig,
                 mergeStats: MergeStats.Persistent.Closed[Iterable])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                                     functionStore: FunctionStore,
                                                                     fileSweeper: FileSweeper,
                                                                     bufferCleaner: ByteBufferSweeperActor,
                                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                     blockCacheSweeper: Option[MemorySweeper.Block],
                                                                     segmentIO: SegmentReadIO,
                                                                     idGenerator: IDGenerator,
                                                                     forceSaveApplier: ForceSaveApplier,
                                                                     ec: ExecutionContext,
                                                                     compactionParallelism: CompactionParallelism): Future[Iterable[PersistentSegment]] =
    SegmentBlock.writeOneOrMany(
      mergeStats = mergeStats,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    ) flatMap {
      transient =>
        SegmentWritePersistentIO.persistTransient(
          pathsDistributor = pathsDistributor,
          segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
          mmap = segmentConfig.mmap,
          transient = transient
        ).toFuture
    }

  def copyToPersist(segment: Segment,
                    createdInLevel: Int,
                    pathsDistributor: PathsDistributor,
                    removeDeletes: Boolean,
                    valuesConfig: ValuesBlockConfig,
                    sortedIndexConfig: SortedIndexBlockConfig,
                    binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                    hashIndexConfig: HashIndexBlockConfig,
                    bloomFilterConfig: BloomFilterBlockConfig,
                    segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                       segmentIO: SegmentReadIO,
                                                       idGenerator: IDGenerator,
                                                       forceSaveApplier: ForceSaveApplier,
                                                       ec: ExecutionContext,
                                                       compactionParallelism: CompactionParallelism): Future[Iterable[PersistentSegment]] =
    segment match {
      case segment: PersistentSegment =>
        Future {
          Slice(
            copyToPersist(
              segment = segment,
              pathsDistributor = pathsDistributor,
              segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
              mmap = segmentConfig.mmap
            )
          )
        }

      case memory: MemorySegment =>
        copyToPersist(
          keyValues = memory.skipList.values(),
          createdInLevel = createdInLevel,
          pathsDistributor = pathsDistributor,
          removeDeletes = removeDeletes,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )
    }

  def copyToPersist(segment: PersistentSegment,
                    pathsDistributor: PathsDistributor,
                    segmentRefCacheLife: SegmentRefCacheLife,
                    mmap: MMAP.Segment)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore,
                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                        fileSweeper: FileSweeper,
                                        bufferCleaner: ByteBufferSweeperActor,
                                        blockCacheSweeper: Option[MemorySweeper.Block],
                                        segmentIO: SegmentReadIO,
                                        idGenerator: IDGenerator,
                                        forceSaveApplier: ForceSaveApplier): PersistentSegment = {
    val nextPath = pathsDistributor.next.resolve(IDGenerator.segment(idGenerator.next))

    segment.copyTo(nextPath)

    try
      Segment(
        path = nextPath,
        formatId = segment.formatId,
        createdInLevel = segment.createdInLevel,
        segmentRefCacheLife = segmentRefCacheLife,
        mmap = mmap,
        minKey = segment.minKey,
        maxKey = segment.maxKey,
        segmentSize = segment.segmentSize,
        minMaxFunctionId = segment.minMaxFunctionId,
        updateCount = segment.updateCount,
        rangeCount = segment.rangeCount,
        putCount = segment.putCount,
        putDeadlineCount = segment.putDeadlineCount,
        keyValueCount = segment.keyValueCount,
        nearestExpiryDeadline = segment.nearestPutDeadline,
        copiedFrom = Some(segment)
      )
    catch {
      case exception: Exception =>
        logger.error("Failed to copyToPersist Segment {}", segment.path, exception)
        try
          Effect.deleteIfExists(nextPath)
        catch {
          case exception: Exception =>
            logger.error(s"Failed to delete copied persistent Segment ${segment.path}", exception)
        }
        throw exception
    }
  }

  def copyToPersist(keyValues: Iterable[Memory],
                    createdInLevel: Int,
                    pathsDistributor: PathsDistributor,
                    removeDeletes: Boolean,
                    valuesConfig: ValuesBlockConfig,
                    sortedIndexConfig: SortedIndexBlockConfig,
                    binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                    hashIndexConfig: HashIndexBlockConfig,
                    bloomFilterConfig: BloomFilterBlockConfig,
                    segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                       segmentIO: SegmentReadIO,
                                                       idGenerator: IDGenerator,
                                                       forceSaveApplier: ForceSaveApplier,
                                                       ec: ExecutionContext,
                                                       compactionParallelism: CompactionParallelism): Future[Iterable[PersistentSegment]] = {
    val builder =
      if (removeDeletes)
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)(KeyValueGrouper.toLastLevelOrNull)
      else
        MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    val closedStats =
      builder
        .addAll(keyValues)
        .close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig,
      mergeStats = closedStats
    )
  }

  def copyToMemory(segment: Segment,
                   createdInLevel: Int,
                   pathsDistributor: PathsDistributor,
                   removeDeletes: Boolean,
                   minSegmentSize: Int,
                   maxKeyValueCountPerSegment: Int,
                   initialiseIteratorsInOneSeek: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          fileSweeper: FileSweeper,
                                                          idGenerator: IDGenerator): Slice[MemorySegment] =
    copyToMemory(
      keyValues = segment.iterator(initialiseIteratorsInOneSeek),
      pathsDistributor = pathsDistributor,
      removeDeletes = removeDeletes,
      minSegmentSize = minSegmentSize,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      createdInLevel = createdInLevel
    )

  def copyToMemory(keyValues: Iterator[KeyValue],
                   pathsDistributor: PathsDistributor,
                   removeDeletes: Boolean,
                   minSegmentSize: Int,
                   maxKeyValueCountPerSegment: Int,
                   createdInLevel: Int)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore,
                                        fileSweeper: FileSweeper,
                                        idGenerator: IDGenerator): Slice[MemorySegment] = {
    val builder =
      new MergeStats.Memory.ClosedIgnoreStats[Iterator](
        isEmpty = false,
        keyValues = Segment.toMemoryIterator(keyValues, removeDeletes)
      )

    Segment.memory(
      minSegmentSize = minSegmentSize,
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      maxKeyValueCountPerSegment = maxKeyValueCountPerSegment,
      stats = builder
    )
  }

  /**
   * This refresh does not compute new sortedIndex size and uses existing. Saves an iteration
   * and performs refresh instantly.
   */
  def refreshForSameLevel(sortedIndexBlock: SortedIndexBlock,
                          valuesBlock: Option[ValuesBlock],
                          iterator: Iterator[Persistent],
                          keyValuesCount: Int,
                          removeDeletes: Boolean,
                          createdInLevel: Int,
                          valuesConfig: ValuesBlockConfig,
                          sortedIndexConfig: SortedIndexBlockConfig,
                          binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                          hashIndexConfig: HashIndexBlockConfig,
                          bloomFilterConfig: BloomFilterBlockConfig,
                          segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             ec: ExecutionContext,
                                                             compactionParallelism: CompactionParallelism): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] = {

    val sortedIndexSize =
      sortedIndexBlock.compressionInfo match {
        case compressionInfo: BlockCompressionInfo =>
          compressionInfo.decompressedLength

        case BlockCompressionInfo.Null =>
          sortedIndexBlock.offset.size
      }

    val valuesSize =
      valuesBlock match {
        case Some(valuesBlock) =>
          valuesBlock.compressionInfo match {
            case value: BlockCompressionInfo =>
              value.decompressedLength

            case BlockCompressionInfo.Null =>
              valuesBlock.offset.size
          }
        case None =>
          0
      }

    val mergeStats =
      new MergeStats.Persistent.Closed[Iterator](
        isEmpty = false,
        keyValuesCount = keyValuesCount,
        keyValues = Segment.toMemoryIterator(iterator, removeDeletes),
        totalValuesSize = valuesSize,
        maxSortedIndexSize = sortedIndexSize
      )

    SegmentBlock.writeOneOrMany(
      mergeStats = mergeStats,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def refreshForNewLevel(keyValues: Iterator[Persistent],
                         removeDeletes: Boolean,
                         createdInLevel: Int,
                         valuesConfig: ValuesBlockConfig,
                         sortedIndexConfig: SortedIndexBlockConfig,
                         binarySearchIndexConfig: BinarySearchIndexBlockConfig,
                         hashIndexConfig: HashIndexBlockConfig,
                         bloomFilterConfig: BloomFilterBlockConfig,
                         segmentConfig: SegmentBlockConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                            ec: ExecutionContext,
                                                            compactionParallelism: CompactionParallelism): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] =
    Future
      .unit
      .mapUnit {
        MergeStats
          .persistentBuilder(Segment.toMemoryIterator(keyValues, removeDeletes))
          .close(
            hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
            optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
          )
      }
      .flatMap {
        builder =>
          SegmentBlock.writeOneOrMany(
            mergeStats = builder,
            createdInLevel = createdInLevel,
            bloomFilterConfig = bloomFilterConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            sortedIndexConfig = sortedIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )
      }

  def apply(path: Path,
            formatId: Byte,
            createdInLevel: Int,
            segmentRefCacheLife: SegmentRefCacheLife,
            mmap: MMAP.Segment,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            segmentSize: Int,
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            updateCount: Int,
            rangeCount: Int,
            putCount: Int,
            putDeadlineCount: Int,
            keyValueCount: Int,
            nearestExpiryDeadline: Option[Deadline],
            copiedFrom: Option[PersistentSegment],
            checkExists: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore,
                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                         fileSweeper: FileSweeper,
                                         bufferCleaner: ByteBufferSweeperActor,
                                         blockCacheSweeper: Option[MemorySweeper.Block],
                                         segmentIO: SegmentReadIO,
                                         forceSaveApplier: ForceSaveApplier): PersistentSegment = {
    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = true,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          DBFile.standardRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = true,
            checkExists = checkExists
          )
      }

    if (formatId == PersistentSegmentOne.formatId)
      copiedFrom match {
        case Some(one: PersistentSegmentOne) =>
          PersistentSegmentOne(
            file = file,
            minKey = minKey,
            maxKey = maxKey,
            minMaxFunctionId = minMaxFunctionId,
            segmentSize = segmentSize,
            nearestExpiryDeadline = nearestExpiryDeadline,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            keyValueCount = keyValueCount,
            createdInLevel = one.createdInLevel,
            valuesReaderCacheable = one.ref.segmentBlockCache.cachedValuesSliceReader(),
            sortedIndexReaderCacheable = one.ref.segmentBlockCache.cachedSortedIndexSliceReader(),
            hashIndexReaderCacheable = one.ref.segmentBlockCache.cachedHashIndexSliceReader(),
            binarySearchIndexReaderCacheable = one.ref.segmentBlockCache.cachedBinarySearchIndexSliceReader(),
            bloomFilterReaderCacheable = one.ref.segmentBlockCache.cachedBloomFilterSliceReader(),
            footerCacheable = one.ref.segmentBlockCache.cachedFooter(),
            previousBlockCache = one.ref.blockCache()
          )

        case Some(segment: PersistentSegmentMany) =>
          throw new Exception(s"Invalid copy. Copied as ${PersistentSegmentOne.getClass.getSimpleName} but received ${segment.getClass.getSimpleName}.")

        case None =>
          PersistentSegmentOne(
            file = file,
            minKey = minKey,
            maxKey = maxKey,
            minMaxFunctionId = minMaxFunctionId,
            segmentSize = segmentSize,
            nearestExpiryDeadline = nearestExpiryDeadline,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            keyValueCount = keyValueCount,
            createdInLevel = createdInLevel,
            valuesReaderCacheable = None,
            sortedIndexReaderCacheable = None,
            hashIndexReaderCacheable = None,
            binarySearchIndexReaderCacheable = None,
            bloomFilterReaderCacheable = None,
            footerCacheable = None,
            previousBlockCache = None
          )
      }
    else if (formatId == PersistentSegmentMany.formatId)
      copiedFrom match {
        case Some(one: PersistentSegmentOne) =>
          throw new Exception(s"Invalid copy. Copied as ${PersistentSegmentMany.getClass.getSimpleName} but received ${one.getClass.getSimpleName}.")

        case Some(segment: PersistentSegmentMany) =>
          PersistentSegmentMany(
            file = file,
            segmentSize = segmentSize,
            createdInLevel = createdInLevel,
            segmentRefCacheLife = segmentRefCacheLife,
            minKey = minKey,
            maxKey = maxKey,
            minMaxFunctionId = minMaxFunctionId,
            nearestExpiryDeadline = nearestExpiryDeadline,
            updateCount = segment.updateCount,
            rangeCount = segment.rangeCount,
            putCount = segment.putCount,
            putDeadlineCount = segment.putDeadlineCount,
            keyValueCount = segment.keyValueCount,
            copiedFrom = Some(segment)
          )

        case None =>
          PersistentSegmentMany(
            file = file,
            segmentSize = segmentSize,
            createdInLevel = createdInLevel,
            segmentRefCacheLife = segmentRefCacheLife,
            minKey = minKey,
            maxKey = maxKey,
            minMaxFunctionId = minMaxFunctionId,
            nearestExpiryDeadline = nearestExpiryDeadline,
            updateCount = updateCount,
            rangeCount = rangeCount,
            putCount = putCount,
            putDeadlineCount = putDeadlineCount,
            keyValueCount = keyValueCount,
            copiedFrom = None
          )
      }
    else
      throw new Exception(s"Invalid segment formatId: $formatId")
  }

  /**
   * Reads the [[PersistentSegmentOne]] when the min, max keys & fileSize is not known.
   *
   * This function requires the Segment to be opened and read. After the Segment is successfully
   * read the file is closed.
   *
   * This function is only used for Appendix file recovery initialization.
   */
  def apply(path: Path,
            mmap: MMAP.Segment,
            checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                  fileSweeper: FileSweeper,
                                  bufferCleaner: ByteBufferSweeperActor,
                                  forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    implicit val segmentIO: SegmentReadIO = SegmentReadIO.defaultSynchronisedStoredIfCompressed

    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          DBFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = false,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          DBFile.standardRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = false,
            checkExists = checkExists
          )
      }

    val formatId = file.getSkipCache(position = 0)

    val segment =
      if (formatId == PersistentSegmentOne.formatId)
        PersistentSegmentOne(file = file)
      else if (formatId == PersistentSegmentMany.formatId)
      //NOTE - SegmentRefCacheLife is Permanent because this function use by AppendixRepairer
        PersistentSegmentMany(file = file, segmentRefCacheLife = SegmentRefCacheLife.Permanent)
      else
        throw new Exception(s"Invalid Segment formatId: $formatId")

    file.close()

    segment
  }

  def segmentSizeForMerge(segment: Segment,
                          initialiseIteratorsInOneSeek: Boolean): Int =
    segment match {
      case segment: MemorySegment =>
        segment.segmentSize

      case segment: PersistentSegmentOne =>
        segmentSizeForMerge(segment.ref)

      case segment: PersistentSegmentMany =>
        val listSegmentSize = segmentSizeForMerge(segment.listSegmentCache.value(()))

        //1+ for formatId
        segment.segmentRefs(initialiseIteratorsInOneSeek).foldLeft(1 + listSegmentSize) {
          case (size, ref) =>
            size + segmentSizeForMerge(ref)
        }
    }

  def segmentSizeForMerge(segment: SegmentRef): Int = {
    val footer = segment.getFooter()
    footer.sortedIndexOffset.size +
      footer.valuesOffset.map(_.size).getOrElse(0)
  }

  def keyOverlaps(keyValue: KeyValue,
                  segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyOverlaps(
      keyValue = keyValue,
      minKey = segment.minKey,
      maxKey = segment.maxKey
    )

  def keyOverlaps(keyValue: KeyValue,
                  minKey: Slice[Byte],
                  maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
    import keyOrder._
    keyValue.key >= minKey && {
      if (maxKey.inclusive)
        keyValue.key <= maxKey.maxKey
      else
        keyValue.key < maxKey.maxKey
    }
  }

  def overlaps(assignable: Assignable,
               minKey: Slice[Byte],
               maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    assignable match {
      case keyValue: KeyValue =>
        keyOverlaps(
          keyValue = keyValue,
          minKey = minKey,
          maxKey = maxKey
        )

      case collection: Assignable.Collection =>
        overlaps(
          minKey = collection.key,
          maxKey = collection.maxKey.maxKey,
          maxKeyInclusive = collection.maxKey.inclusive,
          targetMinKey = minKey,
          targetMaxKey = maxKey
        )
    }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    overlaps(
      minKey = minKey,
      maxKey = maxKey,
      maxKeyInclusive = maxKeyInclusive,
      targetMinKey = segment.minKey,
      targetMaxKey = segment.maxKey
    )

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               targetMinKey: Slice[Byte],
               targetMaxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, maxKeyInclusive), (targetMinKey, targetMaxKey.maxKey, targetMaxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, maxKeyInclusive, segment))

  def overlaps(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    keyValues match {
      case util.Left(value) =>
        overlaps(value, segments)

      case util.Right(value) =>
        overlaps(value, segments)
    }

  def overlaps(keyValues: Slice[Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(keyValues) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
    }

  def overlaps(segment1: Assignable.Collection,
               segment2: Assignable.Collection)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects(
      range1 = (segment1.key, segment1.maxKey.maxKey, segment1.maxKey.inclusive),
      range2 = (segment2.key, segment2.maxKey.maxKey, segment2.maxKey.inclusive)
    )

  def partitionOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                       segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): (Iterable[A], Iterable[A]) =
    segments1.partition(segmentToWrite => segments2.exists(existingSegment => Segment.overlaps(segmentToWrite, existingSegment)))

  def nonOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                 segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping[A <: Assignable.Collection](segments1: Iterable[A],
                                                 segments2: Iterable[A],
                                                 count: Int)(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    if (count == 0) {
      Iterable.empty
    } else {
      val resultSegments = ListBuffer.empty[A]
      segments1 foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }

  def overlaps[A <: Assignable.Collection](segments1: Iterable[A],
                                           segments2: Iterable[A])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[A] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def overlaps(segment: Assignable.Collection,
               segments2: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments2.exists(segment2 => overlaps(segment, segment2))

  def overlapsCount(segment: Assignable.Collection,
                    segments2: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Int =
    segments2.count(segment2 => overlaps(segment, segment2))

  def containsOne(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  def contains(segment: Segment, segments2: Iterable[Segment]): Boolean =
    segments2.exists(_.path == segment.path)

  def deleteSegments(segments: Iterable[Segment]): Int =
    segments.foldLeftRecover(0, failFast = false) {
      case (deleteCount, segment) =>
        segment.delete
        deleteCount + 1
    }

  //NOTE: segments should be ordered.
  def tempMinMaxKeyValues(segments: Iterable[Assignable.Collection]): Slice[Memory] =
    segments.foldLeft(Slice.of[Memory](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.key, Slice.Null, None, Time.empty)
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, Slice.Null, None, Time.empty)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, Value.FromValue.Null, Value.Update(maxKey, None, Time.empty))
        }
    }

  @inline def tempMinMaxKeyValuesFrom[I](map: I, head: I => Option[Memory], last: I => Option[Memory]): Slice[Memory] = {
    for {
      minKey <- head(map).map(memory => Memory.Put(memory.key, Slice.Null, None, Time.empty))
      maxKey <- last(map) map {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, Slice.Null, None, Time.empty)

        case Memory.Range(fromKey, toKey, _, _) =>
          Memory.Range(fromKey, toKey, Value.FromValue.Null, Value.Update(Slice.Null, None, Time.empty))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.of[Memory](0)

  def tempMinMaxKeyValues(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  def tempMinMaxKeyValues(keyValues: Slice[Memory]): Slice[Memory] =
    tempMinMaxKeyValuesFrom[Slice[Memory]](keyValues, _.headOption, _.lastOption)

  @inline def minMaxKeyFrom[I](input: I, head: I => Option[Memory], last: I => Option[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- head(input).map(_.key)
      maxKey <- last(input) map {
        case fixed: Memory.Fixed =>
          (fixed.key, true)

        case range: Memory.Range =>
          (range.toKey, false)
      }
    } yield (minKey, maxKey._1, maxKey._2)

  def minMaxKey(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    keyValues match {
      case util.Left(value) =>
        minMaxKey(value)

      case util.Right(value) =>
        minMaxKey(value)
    }

  @inline def minMaxKey(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]](map, _.head().toOptionS, _.last().toOptionS)

  @inline def minMaxKey(map: Slice[Memory]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    minMaxKeyFrom[Slice[Memory]](map, _.headOption, _.lastOption)

  def minMaxKey(segment: Iterable[Assignable.Collection]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- segment.headOption.map(_.key)
      maxKey <- segment.lastOption.map(_.maxKey) map {
        case MaxKey.Fixed(maxKey) =>
          (maxKey, true)

        case MaxKey.Range(_, maxKey) =>
          (maxKey, false)
      }
    } yield {
      (minKey, maxKey._1, maxKey._2)
    }

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Iterable[Assignable.Collection])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    right match {
      case util.Left(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

      case util.Right(right) =>
        Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))
    }

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Assignable.Collection],
                right: Slice[Memory])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      val assignments =
        Assigner.assignMinMaxOnlyUnsafeNoGaps(
          inputSegments = inputSegments,
          targetSegments = appendixSegments
        )

      Segment.overlaps(
        segments1 = busySegments,
        segments2 = assignments
      ).nonEmpty
    }

  def overlapsWithBusySegments(map: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    if (busySegments.isEmpty)
      false
    else {
      for {
        head <- map.head().toOptionS
        last <- map.last().toOptionS
      } yield {
        val assignments =
          if (keyOrder.equiv(head.key, last.key))
            Assigner.assignUnsafeNoGaps(keyValues = Slice(head), segments = appendixSegments, initialiseIteratorsInOneSeek = false)
          else
            Assigner.assignUnsafeNoGaps(keyValues = Slice(head, last), segments = appendixSegments, initialiseIteratorsInOneSeek = false)

        Segment.overlaps(
          segments1 = busySegments,
          segments2 = assignments.map(_.segment)
        ).nonEmpty
      }
    } getOrElse false

  def getNearestPutDeadline(deadline: Option[Deadline],
                            next: KeyValue): Option[Deadline] =
    next match {
      case readOnly: KeyValue.Put =>
        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)

      case _: KeyValue.Remove =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Update =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.PendingApply =>
        //        FiniteDurations.getNearestDeadline(deadline, readOnly.deadline)
        deadline

      case _: KeyValue.Function =>
        deadline

      case range: KeyValue.Range =>
        range.fetchFromAndRangeValueUnsafe match {
          case (fromValue: Value.FromValue, _) =>
            getNearestPutDeadline(deadline, fromValue)

          case (Value.FromValue.Null, _) =>
            deadline
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Memory): Option[Deadline] =
    keyValue match {
      case writeOnly: Memory.Fixed =>
        FiniteDurations.getNearestDeadline(deadline, writeOnly.deadline)

      case range: Memory.Range =>
        (range.fromValue, range.rangeValue) match {
          case (fromValue: Value.FromValue, rangeValue) =>
            val fromValueDeadline = getNearestPutDeadline(deadline, fromValue)
            getNearestPutDeadline(fromValueDeadline, rangeValue)

          case (Value.FromValue.Null, rangeValue) =>
            getNearestPutDeadline(deadline, rangeValue)
        }
    }

  def getNearestPutDeadline(deadline: Option[Deadline],
                            keyValue: Value.FromValue): Option[Deadline] =
    keyValue match {
      case _: Value.RangeValue =>
        //        getNearestDeadline(deadline, rangeValue)
        deadline

      case put: Value.Put =>
        FiniteDurations.getNearestDeadline(deadline, put.deadline)
    }

  //  def getNearestDeadline(deadline: Option[Deadline],
  //                         rangeValue: Value.RangeValue): Option[Deadline] =
  //    rangeValue match {
  //      case remove: Value.Remove =>
  //        FiniteDurations.getNearestDeadline(deadline, remove.deadline)
  //      case update: Value.Update =>
  //        FiniteDurations.getNearestDeadline(deadline, update.deadline)
  //      case _: Value.Function =>
  //        deadline
  //      case pendingApply: Value.PendingApply =>
  //        FiniteDurations.getNearestDeadline(deadline, pendingApply.deadline)
  //    }

  //  def getNearestDeadline(previous: Option[Deadline],
  //                         applies: Slice[Value.Apply]): Option[Deadline] =
  //    applies.foldLeft(previous) {
  //      case (deadline, apply) =>
  //        getNearestDeadline(
  //          deadline = deadline,
  //          rangeValue = apply
  //        )
  //    }

  def getNearestDeadline(keyValues: Iterable[KeyValue]): Option[Deadline] =
    keyValues.foldLeftRecover(Option.empty[Deadline])(getNearestPutDeadline)

  def getNearestDeadlineSegment(previous: Segment,
                                next: Segment): SegmentOption =
    (previous.nearestPutDeadline, next.nearestPutDeadline) match {
      case (None, None) => Segment.Null
      case (Some(_), None) => previous
      case (None, Some(_)) => next
      case (Some(previousDeadline), Some(nextDeadline)) =>
        if (previousDeadline < nextDeadline)
          previous
        else
          next
    }

  def getNearestDeadlineSegment(segments: Iterable[Segment]): SegmentOption =
    segments.foldLeft(Segment.Null: SegmentOption) {
      case (previous, next) =>
        previous mapS {
          previous =>
            getNearestDeadlineSegment(previous, next)
        } getOrElse {
          if (next.nearestPutDeadline.isDefined)
            next
          else
            Segment.Null
        }
    }

  def toMemoryIterator(fullIterator: Iterator[KeyValue],
                       removeDeletes: Boolean): Iterator[Memory] =
    new Iterator[Memory] {

      var nextOne: Memory = _

      //FIXME - hasNext jumps to next item even if next() was not invoked.
      @tailrec
      final override def hasNext: Boolean =
        if (fullIterator.hasNext) {
          val nextKeyValue = fullIterator.next()
          val nextKeyValueOrNull =
            if (removeDeletes)
              KeyValueGrouper.toLastLevelOrNull(nextKeyValue)
            else
              nextKeyValue.toMemory()

          if (nextKeyValueOrNull == null) {
            hasNext
          } else {
            nextOne = nextKeyValueOrNull
            true
          }
        } else {
          false
        }

      override def next(): Memory =
        nextOne
    }
}

private[core] trait Segment extends FileSweeperItem with SegmentOption with Assignable.Collection { self =>

  final def key: Slice[Byte] =
    minKey

  def minKey: Slice[Byte]

  def maxKey: MaxKey[Slice[Byte]]

  def segmentSize: Int

  def nearestPutDeadline: Option[Deadline]

  def minMaxFunctionId: Option[MinMax[Slice[Byte]]]

  def formatId: Byte

  def createdInLevel: Int

  def path: Path

  def isMMAP: Boolean

  def segmentNumber: Long =
    Effect.numberFileId(path)._1

  def getFromCache(key: Slice[Byte]): KeyValueOption

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean

  def mightContainFunction(key: Slice[Byte]): Boolean

  def get(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def lower(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def higher(key: Slice[Byte], threadState: ThreadReadState): KeyValueOption

  def iterator(initialiseIteratorsInOneSeek: Boolean): Iterator[KeyValue]

  def delete: Unit

  def delete(delay: FiniteDuration): Unit

  def close: Unit

  def keyValueCount: Int

  def clearCachedKeyValues(): Unit

  def clearAllCaches(): Unit

  def isInKeyValueCache(key: Slice[Byte]): Boolean

  def isKeyValueCacheEmpty: Boolean

  def areAllCachesEmpty: Boolean

  def cachedKeyValueSize: Int

  def hasRange: Boolean =
    rangeCount > 0

  def updateCount: Int

  def rangeCount: Int

  def putCount: Int

  def putDeadlineCount: Int

  def hasExpired(): Boolean =
    nearestPutDeadline.exists(_.isOverdue())

  def hasUpdateOrRange: Boolean =
    updateCount > 0 || rangeCount > 0

  def hasUpdateOrRangeOrExpired(): Boolean =
    hasUpdateOrRange || hasExpired()

  def isFooterDefined: Boolean

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean

  def existsOnDiskOrMemory: Boolean

  def hasBloomFilter: Boolean

  override def isNoneS: Boolean =
    false

  override def getS: Segment =
    this

  override def equals(other: Any): Boolean =
    other match {
      case other: Segment =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}

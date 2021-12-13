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
import swaydb.config.{MMAP, SegmentRefCacheLife}
import swaydb.core.file.{CoreFile, ForceSaveApplier}
import swaydb.core.file.sweeper.FileSweeper
import swaydb.core.file.sweeper.bytebuffer.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockConfig}
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockConfig}
import swaydb.core.segment.block.values.{ValuesBlock, ValuesBlockConfig}
import swaydb.core.segment.block.BlockCompressionInfo
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.{SegmentKeyOrders, Memory, Persistent}
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.merge.KeyValueGrouper
import swaydb.core.segment.io.{SegmentCompactionIO, SegmentReadIO, SegmentWritePersistentIO}
import swaydb.core.util.{DefIO, MinMax}
import swaydb.effect.Effect
import swaydb.slice.{MaxKey, Slice}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.{Aggregator, IDGenerator}
import swaydb.utils.Futures._

import java.nio.file.Path
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Deadline

private[core] sealed trait PersistentSegmentOption {
  def asSegmentOption: SegmentOption
}

private[core] trait PersistentSegment extends Segment with PersistentSegmentOption {
  def file: CoreFile

  def copyTo(toPath: Path): Path

  final def isMMAP: Boolean =
    file.memoryMapped

  override def asSegmentOption: SegmentOption =
    this

  override def existsOnDiskOrMemory(): Boolean =
    this.existsOnDisk()

  def put(headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          newKeyValues: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlockConfig,
          sortedIndexConfig: SortedIndexBlockConfig,
          binarySearchIndexConfig: BinarySearchIndexBlockConfig,
          hashIndexConfig: HashIndexBlockConfig,
          bloomFilterConfig: BloomFilterBlockConfig,
          segmentConfig: SegmentBlockConfig,
          pathsDistributor: PathsDistributor,
          segmentRefCacheLife: SegmentRefCacheLife,
          mmap: MMAP.Segment)(implicit idGenerator: IDGenerator,
                              executionContext: ExecutionContext,
                              compactionIO: SegmentCompactionIO.Actor): Future[DefIO[PersistentSegmentOption, Iterable[PersistentSegment]]]

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlockConfig,
              sortedIndexConfig: SortedIndexBlockConfig,
              binarySearchIndexConfig: BinarySearchIndexBlockConfig,
              hashIndexConfig: HashIndexBlockConfig,
              bloomFilterConfig: BloomFilterBlockConfig,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                                 ec: ExecutionContext): Future[DefIO[PersistentSegment, Slice[TransientSegment.OneOrRemoteRefOrMany]]]


}

private[core] object PersistentSegment extends LazyLogging {

  case object Null extends PersistentSegmentOption {
    override val asSegmentOption: SegmentOption = Segment.Null
  }

  def apply(pathsDistributor: PathsDistributor,
            createdInLevel: Int,
            bloomFilterConfig: BloomFilterBlockConfig,
            hashIndexConfig: HashIndexBlockConfig,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig,
            sortedIndexConfig: SortedIndexBlockConfig,
            valuesConfig: ValuesBlockConfig,
            segmentConfig: SegmentBlockConfig,
            mergeStats: MergeStats.Persistent.Closed[Iterable])(implicit keyOrders: SegmentKeyOrders,
                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                functionStore: CoreFunctionStore,
                                                                fileSweeper: FileSweeper,
                                                                bufferCleaner: ByteBufferSweeperActor,
                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                blockCacheSweeper: Option[MemorySweeper.Block],
                                                                segmentIO: SegmentReadIO,
                                                                idGenerator: IDGenerator,
                                                                forceSaveApplier: ForceSaveApplier,
                                                                ec: ExecutionContext): Future[Iterable[PersistentSegment]] = {
    import keyOrders.keyOrder

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
            checkExists: Boolean = true)(implicit keyOrders: SegmentKeyOrders,
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: CoreFunctionStore,
                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                         fileSweeper: FileSweeper,
                                         bufferCleaner: ByteBufferSweeperActor,
                                         blockCacheSweeper: Option[MemorySweeper.Block],
                                         segmentIO: SegmentReadIO,
                                         forceSaveApplier: ForceSaveApplier): PersistentSegment = {
    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          CoreFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = true,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          CoreFile.standardRead(
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
            checkExists: Boolean)(implicit keyOrders: SegmentKeyOrders,
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: CoreFunctionStore,
                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                  fileSweeper: FileSweeper,
                                  bufferCleaner: ByteBufferSweeperActor,
                                  forceSaveApplier: ForceSaveApplier): PersistentSegment = {

    implicit val segmentIO: SegmentReadIO = SegmentReadIO.defaultSynchronisedStoredIfCompressed

    val file =
      mmap match {
        case _: MMAP.On | _: MMAP.ReadOnly =>
          CoreFile.mmapRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = false,
            deleteAfterClean = mmap.deleteAfterClean,
            checkExists = checkExists
          )

        case _: MMAP.Off =>
          CoreFile.standardRead(
            path = path,
            fileOpenIOStrategy = segmentIO.fileOpenIO,
            autoClose = false,
            checkExists = checkExists
          )
      }

    val formatId = file.get(position = 0)

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

  def apply(keyValues: Iterable[Memory],
            createdInLevel: Int,
            pathsDistributor: PathsDistributor,
            removeDeletes: Boolean,
            valuesConfig: ValuesBlockConfig,
            sortedIndexConfig: SortedIndexBlockConfig,
            binarySearchIndexConfig: BinarySearchIndexBlockConfig,
            hashIndexConfig: HashIndexBlockConfig,
            bloomFilterConfig: BloomFilterBlockConfig,
            segmentConfig: SegmentBlockConfig)(implicit keyOrders: SegmentKeyOrders,
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: CoreFunctionStore,
                                               keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                               fileSweeper: FileSweeper,
                                               bufferCleaner: ByteBufferSweeperActor,
                                               blockCacheSweeper: Option[MemorySweeper.Block],
                                               segmentIO: SegmentReadIO,
                                               idGenerator: IDGenerator,
                                               forceSaveApplier: ForceSaveApplier,
                                               ec: ExecutionContext): Future[Iterable[PersistentSegment]] = {
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

    PersistentSegment(
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

  def copyFrom(segment: Segment,
               createdInLevel: Int,
               pathsDistributor: PathsDistributor,
               removeDeletes: Boolean,
               valuesConfig: ValuesBlockConfig,
               sortedIndexConfig: SortedIndexBlockConfig,
               binarySearchIndexConfig: BinarySearchIndexBlockConfig,
               hashIndexConfig: HashIndexBlockConfig,
               bloomFilterConfig: BloomFilterBlockConfig,
               segmentConfig: SegmentBlockConfig)(implicit keyOrders: SegmentKeyOrders,
                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                  functionStore: CoreFunctionStore,
                                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                  fileSweeper: FileSweeper,
                                                  bufferCleaner: ByteBufferSweeperActor,
                                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                                  segmentIO: SegmentReadIO,
                                                  idGenerator: IDGenerator,
                                                  forceSaveApplier: ForceSaveApplier,
                                                  ec: ExecutionContext): Future[Iterable[PersistentSegment]] =
    segment match {
      case segment: PersistentSegment =>
        Future {
          Slice(
            copyFrom(
              segment = segment,
              pathsDistributor = pathsDistributor,
              segmentRefCacheLife = segmentConfig.segmentRefCacheLife,
              mmap = segmentConfig.mmap
            )
          )
        }

      case memory: MemorySegment =>
        import keyOrders.keyOrder

        PersistentSegment(
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

  def copyFrom(segment: PersistentSegment,
               pathsDistributor: PathsDistributor,
               segmentRefCacheLife: SegmentRefCacheLife,
               mmap: MMAP.Segment)(implicit keyOrders: SegmentKeyOrders,
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: CoreFunctionStore,
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
      PersistentSegment(
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
                                                             ec: ExecutionContext): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] = {

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
                                                            ec: ExecutionContext): Future[Slice[TransientSegment.OneOrRemoteRefOrMany]] =
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

}

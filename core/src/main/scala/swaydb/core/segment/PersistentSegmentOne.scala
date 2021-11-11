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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{DBFile, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.BlockCache
import swaydb.core.segment.block.binarysearch.{BinarySearchIndexBlock, BinarySearchIndexBlockOffset, BinarySearchIndexConfig}
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterBlockOffset, BloomFilterConfig}
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockOffset, HashIndexConfig}
import swaydb.core.segment.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.block.segment.{SegmentBlock, SegmentBlockCache, SegmentBlockConfig}
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.defrag.DefragPersistentSegment
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{FileSweeper, MemorySweeper}
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.{MMAP, SegmentRefCacheLife}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import java.nio.file.{Path, Paths}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

protected case object PersistentSegmentOne {

  val formatId = 126.toByte
  val formatIdSlice: Slice[Byte] = Slice(formatId)

  def apply(file: DBFile,
            segment: TransientSegment.OneOrRemoteRef)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      forceSaveApplier: ForceSaveApplier,
                                                      segmentIO: SegmentReadIO): PersistentSegmentOne =
    PersistentSegmentOne(
      file = file,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      minMaxFunctionId = segment.minMaxFunctionId,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestPutDeadline,
      updateCount = segment.updateCount,
      rangeCount = segment.rangeCount,
      putCount = segment.putCount,
      putDeadlineCount = segment.putDeadlineCount,
      keyValueCount = segment.keyValueCount,
      createdInLevel = segment.createdInLevel,
      valuesReaderCacheable = segment.valuesUnblockedReader,
      sortedIndexReaderCacheable = segment.sortedIndexUnblockedReader,
      hashIndexReaderCacheable = segment.hashIndexUnblockedReader,
      binarySearchIndexReaderCacheable = segment.binarySearchUnblockedReader,
      bloomFilterReaderCacheable = segment.bloomFilterUnblockedReader,
      footerCacheable = segment.footerUnblocked,
      previousBlockCache =
        segment match {
          case remote: TransientSegment.RemoteRef =>
            remote.ref.blockCache() orElse BlockCache.forSearch(segment.segmentSize, blockCacheSweeper)

          case _: TransientSegment.One =>
            BlockCache.forSearch(segment.segmentSize, blockCacheSweeper)
        }
    )

  def apply(file: DBFile,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            segmentSize: Int,
            nearestExpiryDeadline: Option[Deadline],
            updateCount: Int,
            rangeCount: Int,
            putCount: Int,
            putDeadlineCount: Int,
            keyValueCount: Int,
            createdInLevel: Int,
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlockOffset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlockOffset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock],
            previousBlockCache: Option[BlockCache.State])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                          blockCacheSweeper: Option[MemorySweeper.Block],
                                                          fileSweeper: FileSweeper,
                                                          bufferCleaner: ByteBufferSweeperActor,
                                                          forceSaveApplier: ForceSaveApplier,
                                                          segmentIO: SegmentReadIO): PersistentSegmentOne = {

    val fileSize = segmentSize - 1

    val segmentBlockRef =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = fileSize,
        blockCache = previousBlockCache orElse BlockCache.forSearch(fileSize, blockCacheSweeper)
      )

    val ref =
      SegmentRef(
        path = file.path,
        minKey = minKey,
        maxKey = maxKey,
        nearestPutDeadline = nearestExpiryDeadline,
        minMaxFunctionId = minMaxFunctionId,
        blockRef = segmentBlockRef,
        segmentIO = segmentIO,
        updateCount = updateCount,
        rangeCount = rangeCount,
        putCount = putCount,
        createdInLevel = createdInLevel,
        putDeadlineCount = putDeadlineCount,
        keyValueCount = keyValueCount,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )

    new PersistentSegmentOne(
      file = file,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      ref = ref
    )
  }

  def apply(file: DBFile)(implicit keyOrder: KeyOrder[Slice[Byte]],
                          timeOrder: TimeOrder[Slice[Byte]],
                          functionStore: FunctionStore,
                          blockCacheSweeper: Option[MemorySweeper.Block],
                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                          fileSweeper: FileSweeper,
                          bufferCleaner: ByteBufferSweeperActor,
                          forceSaveApplier: ForceSaveApplier,
                          segmentIO: SegmentReadIO): PersistentSegment = {

    val fileSize = file.fileSize.toInt

    val refReader =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = fileSize - 1,
        blockCache = None
      )

    val segmentBlockCache =
      SegmentBlockCache(
        path = Paths.get("Reading segment"),
        segmentIO = segmentIO,
        blockRef = refReader,
        valuesReaderCacheable = None,
        sortedIndexReaderCacheable = None,
        hashIndexReaderCacheable = None,
        binarySearchIndexReaderCacheable = None,
        bloomFilterReaderCacheable = None,
        footerCacheable = None
      )

    val footer = segmentBlockCache.getFooter()
    val sortedIndexReader = segmentBlockCache.createSortedIndexReader()
    val valuesReaderOrNull = segmentBlockCache.createValuesReaderOrNull()

    val keyValues =
      SortedIndexBlock.toSlice(
        keyValueCount = footer.keyValueCount,
        sortedIndexReader = sortedIndexReader,
        valuesReaderOrNull = valuesReaderOrNull
      )

    val deadlineMinMaxFunctionId = DeadlineAndFunctionId(keyValues)

    PersistentSegmentOne(
      file = file,
      createdInLevel = footer.createdInLevel,
      minKey = keyValues.head.key.unslice(),
      maxKey =
        keyValues.last match {
          case fixed: KeyValue.Fixed =>
            MaxKey.Fixed(fixed.key.unslice())

          case range: KeyValue.Range =>
            MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())
        },
      minMaxFunctionId = deadlineMinMaxFunctionId.minMaxFunctionId,
      segmentSize = fileSize,
      nearestExpiryDeadline = deadlineMinMaxFunctionId.nearestDeadline,
      updateCount = footer.updateCount,
      rangeCount = footer.rangeCount,
      putCount = footer.putCount,
      putDeadlineCount = footer.putDeadlineCount,
      keyValueCount = footer.keyValueCount,
      valuesReaderCacheable = segmentBlockCache.cachedValuesSliceReader(),
      sortedIndexReaderCacheable = segmentBlockCache.cachedSortedIndexSliceReader(),
      hashIndexReaderCacheable = segmentBlockCache.cachedHashIndexSliceReader(),
      binarySearchIndexReaderCacheable = segmentBlockCache.cachedBinarySearchIndexSliceReader(),
      bloomFilterReaderCacheable = segmentBlockCache.cachedBloomFilterSliceReader(),
      footerCacheable = segmentBlockCache.cachedFooter(),
      previousBlockCache = None
    )
  }
}

protected case class PersistentSegmentOne(file: DBFile,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                          segmentSize: Int,
                                          nearestPutDeadline: Option[Deadline],
                                          private[segment] val ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                                functionStore: FunctionStore,
                                                                                blockCacheSweeper: Option[MemorySweeper.Block],
                                                                                fileSweeper: FileSweeper,
                                                                                bufferCleaner: ByteBufferSweeperActor,
                                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                forceSaveApplier: ForceSaveApplier,
                                                                                segmentIO: SegmentReadIO) extends PersistentSegment with LazyLogging {

  override def formatId: Byte = PersistentSegmentOne.formatId

  def path = file.path

  override def createdInLevel: Int =
    ref.createdInLevel

  override def close: Unit = {
    file.close()
    ref.clearAllCaches()
  }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def delete(delay: FiniteDuration): Unit = {
    val deadline = delay.fromNow
    if (deadline.isOverdue())
      this.delete
    else
      fileSweeper send FileSweeper.Command.Delete(this, deadline)
  }

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure.value.exception)
    } map {
      _ =>
        ref.clearAllCaches()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegmentOne]]'s parent directory.
   */
  def put(headGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          tailGap: Iterable[Assignable.Gap[MergeStats.Persistent.Builder[Memory, ListBuffer]]],
          newKeyValues: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexConfig,
          hashIndexConfig: HashIndexConfig,
          bloomFilterConfig: BloomFilterConfig,
          segmentConfig: SegmentBlockConfig,
          pathsDistributor: PathsDistributor,
          segmentRefCacheLife: SegmentRefCacheLife,
          mmap: MMAP.Segment)(implicit idGenerator: IDGenerator,
                              executionContext: ExecutionContext,
                              compactionIO: CompactionIO.Actor,
                              compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegmentOption, Iterable[PersistentSegment]]] = {
    implicit val valuesConfigImplicit: ValuesBlock.Config = valuesConfig
    implicit val sortedIndexConfigImplicit: SortedIndexBlock.Config = sortedIndexConfig
    implicit val binarySearchIndexConfigImplicit: BinarySearchIndexConfig = binarySearchIndexConfig
    implicit val hashIndexConfigImplicit: HashIndexConfig = hashIndexConfig
    implicit val bloomFilterConfigImplicit: BloomFilterConfig = bloomFilterConfig
    implicit val segmentConfigImplicit: SegmentBlockConfig = segmentConfig

    DefragPersistentSegment.runOnSegment(
      segment = this,
      nullSegment = PersistentSegment.Null,
      headGap = headGap,
      tailGap = tailGap,
      newKeyValues = newKeyValues,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      pathsDistributor = pathsDistributor,
      segmentRefCacheLife = segmentRefCacheLife,
      mmap = mmap
    )
  }

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexConfig,
              hashIndexConfig: HashIndexConfig,
              bloomFilterConfig: BloomFilterConfig,
              segmentConfig: SegmentBlockConfig)(implicit idGenerator: IDGenerator,
                                                  ec: ExecutionContext,
                                                  compactionParallelism: CompactionParallelism): Future[DefIO[PersistentSegment, Slice[TransientSegment.OneOrRemoteRefOrMany]]] = {
    //    val footer = ref.getFooter()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.

    //NOTE - IGNORE created in same Level as configurations can change on boot-up.
    //    if (footer.createdInLevel == createdInLevel)
    //      Segment.refreshForSameLevel(
    //        sortedIndexBlock = ref.segmentBlockCache.getSortedIndex(),
    //        valuesBlock = ref.segmentBlockCache.getValues(),
    //        iterator = iterator,
    //        keyValuesCount = footer.keyValueCount,
    //        removeDeletes = removeDeletes,
    //        createdInLevel = createdInLevel,
    //        valuesConfig = valuesConfig,
    //        sortedIndexConfig = sortedIndexConfig,
    //        binarySearchIndexConfig = binarySearchIndexConfig,
    //        hashIndexConfig = hashIndexConfig,
    //        bloomFilterConfig = bloomFilterConfig,
    //        segmentConfig = segmentConfig
    //      )
    //    else

    Segment.refreshForNewLevel(
      keyValues = ref.iterator(segmentConfig.initialiseIteratorsInOneSeek),
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    ) map {
      refreshed =>
        DefIO(
          input = this,
          output = refreshed
        )
    }
  }

  def getFromCache(key: Slice[Byte]): PersistentOption =
    ref getFromCache key

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean =
    ref.mightContainKey(key, threadState)

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    ref.get(key, threadState)

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    ref.lower(key, threadState)

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    ref.higher(key, threadState)

  def iterator(inOneSeek: Boolean): Iterator[Persistent] =
    ref.iterator(inOneSeek)

  def keyValueCount: Int =
    ref.keyValueCount

  override def isFooterDefined: Boolean =
    ref.isFooterDefined

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    false

  def persistent: Boolean =
    true

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  def hasBloomFilter: Boolean =
    ref.hasBloomFilter

  def clearCachedKeyValues(): Unit =
    ref.clearCachedKeyValues()

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    ref.clearAllCaches()
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    ref isInKeyValueCache key

  def isKeyValueCacheEmpty: Boolean =
    ref.isKeyValueCacheEmpty

  def areAllCachesEmpty: Boolean =
    ref.areAllCachesEmpty

  def cachedKeyValueSize: Int =
    ref.cachedKeyValueSize

  override def updateCount: Int =
    ref.updateCount

  override def rangeCount: Int =
    ref.rangeCount

  override def putCount: Int =
    ref.putCount

  override def putDeadlineCount: Int =
    ref.putDeadlineCount
}

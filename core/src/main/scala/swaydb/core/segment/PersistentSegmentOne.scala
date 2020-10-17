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

package swaydb.core.segment

import java.nio.file.{Path, Paths}

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{KeyValue, Persistent, PersistentOption}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile, ForceSaveApplier}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.data.TransientSegment
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.segment.{SegmentBlock, SegmentBlockCache}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

protected object PersistentSegmentOne {

  val formatId = 126.toByte
  val formatIdSlice = Slice(formatId)
  val formatIdSliceSlice = Slice(formatIdSlice)

  def apply(file: DBFile,
            createdInLevel: Int,
            segment: TransientSegment.One)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                           timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore,
                                           keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                           blockCache: Option[BlockCache.State],
                                           fileSweeper: FileSweeperActor,
                                           bufferCleaner: ByteBufferSweeperActor,
                                           forceSaveApplier: ForceSaveApplier,
                                           segmentIO: SegmentIO): PersistentSegmentOne =
    PersistentSegmentOne(
      file = file,
      createdInLevel = createdInLevel,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      minMaxFunctionId = segment.minMaxFunctionId,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestPutDeadline,
      valuesReaderCacheable = segment.valuesUnblockedReader,
      sortedIndexReaderCacheable = segment.sortedIndexUnblockedReader,
      hashIndexReaderCacheable = segment.hashIndexUnblockedReader,
      binarySearchIndexReaderCacheable = segment.binarySearchUnblockedReader,
      bloomFilterReaderCacheable = segment.bloomFilterUnblockedReader,
      footerCacheable = segment.footerUnblocked
    )

  def apply(file: DBFile,
            createdInLevel: Int,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            segmentSize: Int,
            nearestExpiryDeadline: Option[Deadline],
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         timeOrder: TimeOrder[Slice[Byte]],
                                                         functionStore: FunctionStore,
                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                         blockCache: Option[BlockCache.State],
                                                         fileSweeper: FileSweeperActor,
                                                         bufferCleaner: ByteBufferSweeperActor,
                                                         forceSaveApplier: ForceSaveApplier,
                                                         segmentIO: SegmentIO): PersistentSegmentOne = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val segmentBlockRef =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = segmentSize - 1
      )

    val ref =
      SegmentRef(
        path = file.path,
        minKey = minKey,
        maxKey = maxKey,
        blockRef = segmentBlockRef,
        segmentIO = segmentIO,
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )

    new PersistentSegmentOne(
      file = file,
      createdInLevel = createdInLevel,
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
                          blockCache: Option[BlockCache.State],
                          keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                          fileSweeper: FileSweeperActor,
                          bufferCleaner: ByteBufferSweeperActor,
                          forceSaveApplier: ForceSaveApplier,
                          segmentIO: SegmentIO): PersistentSegment = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val fileSize = file.fileSize.toInt

    val refReader =
      BlockRefReader(
        file = file,
        start = 1,
        fileSize = fileSize - 1
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
      valuesReaderCacheable = segmentBlockCache.valuesReaderCacheable,
      sortedIndexReaderCacheable = segmentBlockCache.sortedIndexReaderCacheable,
      hashIndexReaderCacheable = segmentBlockCache.hashIndexReaderCacheable,
      binarySearchIndexReaderCacheable = segmentBlockCache.binarySearchIndexReaderCacheable,
      bloomFilterReaderCacheable = segmentBlockCache.bloomFilterReaderCacheable,
      footerCacheable = segmentBlockCache.footerCacheable
    )
  }
}

protected case class PersistentSegmentOne(file: DBFile,
                                          createdInLevel: Int,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                          segmentSize: Int,
                                          nearestPutDeadline: Option[Deadline],
                                          private[segment] val ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                                functionStore: FunctionStore,
                                                                                blockCache: Option[BlockCache.State],
                                                                                fileSweeper: FileSweeperActor,
                                                                                bufferCleaner: ByteBufferSweeperActor,
                                                                                keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                forceSaveApplier: ForceSaveApplier,
                                                                                segmentIO: SegmentIO) extends PersistentSegment with LazyLogging {

  implicit val segmentCacheImplicit: SegmentRef = ref
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  override def formatId: Byte = PersistentSegmentOne.formatId

  def path = file.path

  override def close: Unit = {
    file.close()
    ref.clearBlockCache()
  }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def deleteSegmentsEventually =
    fileSweeper send FileSweeper.Command.Delete(this)

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure.value.exception)
    } map {
      _ =>
        ref.clearBlockCache()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegmentOne]]'s parent directory.
   */
  def put(headGap: Iterable[Assignable],
          tailGap: Iterable[Assignable],
          mergeableCount: Int,
          mergeable: Iterator[Assignable],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[PersistentSegment] = {

    val segments =
      SegmentRef.put(
        ref = ref,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      mmap = segmentConfig.mmap,
      segments = segments
    )
  }

  def refresh(removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[PersistentSegment] = {

    val segments =
      SegmentRef.refresh(
        ref = ref,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      mmap = segmentConfig.mmap,
      segments = segments
    )
  }

  def getFromCache(key: Slice[Byte]): PersistentOption =
    ref getFromCache key

  def mightContainKey(key: Slice[Byte]): Boolean =
    ref mightContain key

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.get(key, threadState)

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.lower(key, threadState)

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.higher(key, threadState)

  def toSlice(): Slice[Persistent] =
    ref.toSlice()

  def iterator(): Iterator[Persistent] =
    ref.iterator()

  override def hasRange: Boolean =
    ref.hasRange

  override def hasPut: Boolean =
    ref.hasPut

  def getKeyValueCount(): Int =
    ref.getKeyValueCount()

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
    ref.clearBlockCache()
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    ref isInKeyValueCache key

  def isKeyValueCacheEmpty: Boolean =
    ref.isKeyValueCacheEmpty

  def areAllCachesEmpty: Boolean =
    ref.areAllCachesEmpty

  def cachedKeyValueSize: Int =
    ref.cacheSize

}

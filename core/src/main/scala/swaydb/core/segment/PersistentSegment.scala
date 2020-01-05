/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{KeyValue, Memory, Persistent, PersistentOptional}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.{SegmentBlock, _}
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Aggregator, IO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Deadline
import scala.collection.compat._

object PersistentSegment {
  def apply(file: DBFile,
            createdInLevel: Int,
            mmapReads: Boolean,
            mmapWrites: Boolean,
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
                                                         fileSweeper: FileSweeper.Enabled,
                                                         segmentIO: SegmentIO): PersistentSegment = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val ref =
      SegmentRef(
        path = file.path,
        maxKey = maxKey,
        minKey = minKey,
        segmentIO = segmentIO,
        blockRef = BlockRefReader(file, segmentSize),
        valuesReaderCacheable = valuesReaderCacheable,
        sortedIndexReaderCacheable = sortedIndexReaderCacheable,
        hashIndexReaderCacheable = hashIndexReaderCacheable,
        binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
        bloomFilterReaderCacheable = bloomFilterReaderCacheable,
        footerCacheable = footerCacheable
      )

    new PersistentSegment(
      file = file,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestPutDeadline = nearestExpiryDeadline,
      ref = ref
    )
  }
}

private[segment] case class PersistentSegment(file: DBFile,
                                              createdInLevel: Int,
                                              mmapReads: Boolean,
                                              mmapWrites: Boolean,
                                              minKey: Slice[Byte],
                                              maxKey: MaxKey[Slice[Byte]],
                                              minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                              segmentSize: Int,
                                              nearestPutDeadline: Option[Deadline],
                                              private[segment] val ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                    timeOrder: TimeOrder[Slice[Byte]],
                                                                                    functionStore: FunctionStore,
                                                                                    blockCache: Option[BlockCache.State],
                                                                                    fileSweeper: FileSweeper.Enabled,
                                                                                    keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                    segmentIO: SegmentIO) extends Segment with LazyLogging {

  implicit val segmentCacheImplicit: SegmentRef = ref
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

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
    fileSweeper.delete(this)

  def delete: Unit = {
    logger.trace(s"{}: DELETING FILE", path)
    IO(file.delete()) onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure)
    } map {
      _ =>
        ref.clearBlockCache()
    }
  }

  def copyTo(toPath: Path): Path =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegment]]'s parent directory.
   */
  def put(newKeyValues: Slice[KeyValue],
          minSegmentSize: Int,
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](ListBuffer.newBuilder)

    SegmentMerger.merge(
      newKeyValues = newKeyValues,
      oldKeyValuesCount = getKeyValueCount(),
      oldKeyValues = iterator(),
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed = builder.close(sortedIndexConfig.enableAccessPositionIndex)

    Segment.persistent(
      segmentSize = minSegmentSize,
      pathsDistributor = pathsDistributor,
      segmentConfig = segmentConfig,
      createdInLevel = createdInLevel,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      mergeStats = closed,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig
    )
  }

  def refresh(minSegmentSize: Int,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {

    val footer = ref.getFooter()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.
    if (footer.createdInLevel == createdInLevel) {
      val sortedIndexBlock = ref.segmentBlockCache.getSortedIndex()
      val valuesBlock = ref.segmentBlockCache.getValues()

      val sortedIndexSize =
        sortedIndexBlock.compressionInfo match {
          case Some(compressionInfo) =>
            compressionInfo.decompressedLength

          case None =>
            sortedIndexBlock.offset.size
        }

      val valuesSize =
        valuesBlock match {
          case Some(valuesBlock) =>
            valuesBlock.compressionInfo match {
              case Some(value) =>
                value.decompressedLength

              case None =>
                valuesBlock.offset.size
            }
          case None =>
            0
        }

      val keyValues =
        Segment
          .toMemoryIterator(iterator(), removeDeletes)
          .to(Iterable)

      val mergeStats =
        new MergeStats.Persistent.Closed[Iterable](
          isEmpty = false,
          keyValuesCount = footer.keyValueCount,
          keyValues = keyValues,
          totalValuesSize = valuesSize,
          maxSortedIndexSize = sortedIndexSize
        )

      Segment.persistent(
        segmentSize = minSegmentSize,
        pathsDistributor = pathsDistributor,
        segmentConfig = segmentConfig,
        createdInLevel = createdInLevel,
        mmapReads = mmapReads,
        mmapWrites = mmapWrites,
        mergeStats = mergeStats,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig
      )
    } else {
      //if the
      val keyValues =
        Segment
          .toMemoryIterator(iterator(), removeDeletes)
          .to(Iterable)

      val builder =
        MergeStats
          .persistentBuilder(keyValues)
          .close(sortedIndexConfig.enableAccessPositionIndex)

      Segment.persistent(
        segmentSize = minSegmentSize,
        pathsDistributor = pathsDistributor,
        segmentConfig = segmentConfig,
        createdInLevel = createdInLevel,
        mmapReads = mmapReads,
        mmapWrites = mmapWrites,
        mergeStats = builder,
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig
      )
    }
  }

  def getSegmentBlockOffset(): SegmentBlock.Offset =
    SegmentBlock.Offset(0, file.fileSize.toInt)

  def getFromCache(key: Slice[Byte]): PersistentOptional =
    ref getFromCache key

  def mightContainKey(key: Slice[Byte]): Boolean =
    ref mightContain key

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId.exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
    SegmentRef.get(key, readState)

  def lower(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
    SegmentRef.lower(key, readState)

  def higher(key: Slice[Byte], readState: ThreadReadState): PersistentOptional =
    SegmentRef.higher(key, readState)

  def getAll[T](aggregator: Aggregator[KeyValue, T]): Unit =
    ref getAll aggregator

  override def getAll(): Slice[KeyValue] =
    ref.getAll()

  override def iterator(): Iterator[KeyValue] =
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

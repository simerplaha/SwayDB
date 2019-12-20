/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
import swaydb.core.data.{KeyValue, Memory, Persistent}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.BlockRefReader
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

object PersistentSegment {
  def apply(file: DBFile,
            segmentId: Long,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            segmentSize: Int,
            nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                     blockCache: Option[BlockCache.State],
                                                     fileSweeper: FileSweeper.Enabled,
                                                     segmentIO: SegmentIO): PersistentSegment = {

    implicit val blockCacheMemorySweeper: Option[MemorySweeper.Block] = blockCache.map(_.sweeper)

    val segmentCache =
      SegmentCache(
        path = file.path,
        maxKey = maxKey,
        minKey = minKey,
        segmentIO = segmentIO,
        unsliceKey = true,
        blockRef = BlockRefReader(file, segmentSize)
      )

    new PersistentSegment(
      file = file,
      segmentId = segmentId,
      mmapReads = mmapReads,
      mmapWrites = mmapWrites,
      minKey = minKey,
      maxKey = maxKey,
      minMaxFunctionId = minMaxFunctionId,
      segmentSize = segmentSize,
      nearestExpiryDeadline = nearestExpiryDeadline,
      segmentCache = segmentCache
    )
  }
}

private[segment] case class PersistentSegment(file: DBFile,
                                              segmentId: Long,
                                              mmapReads: Boolean,
                                              mmapWrites: Boolean,
                                              minKey: Slice[Byte],
                                              maxKey: MaxKey[Slice[Byte]],
                                              minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                              segmentSize: Int,
                                              nearestExpiryDeadline: Option[Deadline],
                                              private[segment] val segmentCache: SegmentCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                               timeOrder: TimeOrder[Slice[Byte]],
                                                                                               functionStore: FunctionStore,
                                                                                               blockCache: Option[BlockCache.State],
                                                                                               fileSweeper: FileSweeper.Enabled,
                                                                                               keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                                               segmentIO: SegmentIO) extends Segment with LazyLogging {

  def path = file.path

  override def close: Unit = {
    file.close()
    segmentCache.clearBlockCache()
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
        segmentCache.clearBlockCache()
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
      segmentId = segmentId,
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

    val footer = getFooter()
    val sortedIndexSize = footer.sortedIndexOffset.size
    val valuesSize =
      if (footer.valuesOffset.isDefined)
        footer.valuesOffset.get.size
      else
        0

    val keyValues =
      Segment
        .cleanIterator(iterator(), removeDeletes)
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
      segmentId = segmentId,
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
  }

  def getSegmentBlockOffset(): SegmentBlock.Offset =
    SegmentBlock.Offset(0, file.fileSize.toInt)

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    segmentCache getFromCache key

  def mightContainKey(key: Slice[Byte]): Boolean =
    segmentCache mightContain key

  override def mightContainFunction(key: Slice[Byte]): Boolean =
    minMaxFunctionId.exists {
      minMaxFunctionId =>
        MinMax.contains(
          key = key,
          minMax = minMaxFunctionId
        )(FunctionStore.order)
    }

  def get(key: Slice[Byte], readState: ReadState): Option[Persistent] =
    segmentCache.get(key, readState)

  def lower(key: Slice[Byte], readState: ReadState): Option[Persistent] =
    segmentCache.lower(key, readState)

  def floorHigherHint(key: Slice[Byte]): Option[Slice[Byte]] =
    segmentCache floorHigherHint key

  def higher(key: Slice[Byte], readState: ReadState): Option[Persistent] =
    segmentCache.higher(key, readState)

  def getAll[T](aggregator: Aggregator[KeyValue, T]): Unit =
    segmentCache getAll aggregator

  override def getAll(): Slice[KeyValue] =
    segmentCache.getAll()

  override def iterator(): Iterator[KeyValue] =
    segmentCache.iterator()

  override def hasRange: Boolean =
    segmentCache.hasRange

  override def hasPut: Boolean =
    segmentCache.hasPut

  def getKeyValueCount(): Int =
    segmentCache.getKeyValueCount()

  def getFooter(): SegmentFooterBlock =
    segmentCache.getFooter()

  override def isFooterDefined: Boolean =
    segmentCache.isFooterDefined

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    false

  def persistent: Boolean =
    true

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  override def createdInLevel: Int =
    segmentCache.createdInLevel

  def hasBloomFilter: Boolean =
    segmentCache.hasBloomFilter

  def clearCachedKeyValues(): Unit =
    segmentCache.clearCachedKeyValues()

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    segmentCache.clearBlockCache()
  }

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    segmentCache isInKeyValueCache key

  def isKeyValueCacheEmpty: Boolean =
    segmentCache.isKeyValueCacheEmpty

  def areAllCachesEmpty: Boolean =
    segmentCache.areAllCachesEmpty

  def cachedKeyValueSize: Int =
    segmentCache.cacheSize
}

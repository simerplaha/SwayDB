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
import swaydb.IO._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.{BlockCache, DBFile}
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.segment.format.a.block.{SegmentBlock, _}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

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
                                                     memorySweeper: Option[MemorySweeper.KeyValue],
                                                     blockCache: Option[BlockCache.State],
                                                     fileSweeper: FileSweeper.Enabled,
                                                     segmentIO: SegmentIO): PersistentSegment = {
    val segmentCache =
      SegmentCache(
        id = file.path.toString,
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
                                              segmentCache: SegmentCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                                          functionStore: FunctionStore,
                                                                          blockCache: Option[BlockCache.State],
                                                                          fileSweeper: FileSweeper.Enabled,
                                                                          memorySweeper: Option[MemorySweeper.KeyValue],
                                                                          segmentIO: SegmentIO) extends Segment with LazyLogging {

  def path = file.path

  def skipList: SkipList[Slice[Byte], Persistent] =
    segmentCache.skipList

  override def close: IO[Error.IO, Unit] =
    file.close map {
      _ =>
        segmentCache.clearLocalAndBlockCache()
    }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def deleteSegmentsEventually =
    fileSweeper.delete(this)

  def delete: IO[swaydb.Error.IO, Unit] = {
    logger.trace(s"{}: DELETING FILE", path)
    file.delete() onLeftSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure)
    } map {
      _ =>
        segmentCache.clearLocalAndBlockCache()
    }
  }

  def copyTo(toPath: Path): IO[Error.IO, Path] =
    file copyTo toPath

  /**
   * Default targetPath is set to this [[PersistentSegment]]'s parent directory.
   */
  def put(newKeyValues: Slice[KeyValue.ReadOnly],
          minSegmentSize: Long,
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {
    val currentKeyValues = getAll()

    val splits =
      SegmentMerger.merge(
        newKeyValues = newKeyValues,
        oldKeyValues = currentKeyValues,
        minSegmentSize = minSegmentSize,
        isLastLevel = removeDeletes,
        forInMemory = false,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )

    splits.mapRecover(
      block =
        keyValues => {
          val segmentId = idGenerator.nextID
          Segment.persistent(
            path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
            segmentId = segmentId,
            segmentConfig = segmentConfig,
            createdInLevel = createdInLevel,
            mmapReads = mmapReads,
            mmapWrites = mmapWrites,
            keyValues = keyValues
          )
        },

      recover =
        (segments: Slice[Segment], _: Throwable) =>
          segments foreach {
            segmentToDelete =>
              segmentToDelete.delete onLeftSideEffect {
                exception =>
                  logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
              }
          }
    )
  }

  def refresh(minSegmentSize: Long,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Slice[Segment] = {
    val currentKeyValues = getAll()
    val splits =
      SegmentMerger.split(
        keyValues = currentKeyValues,
        minSegmentSize = minSegmentSize,
        isLastLevel = removeDeletes,
        forInMemory = false,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig
      )

    splits.mapRecover(
      block =
        keyValues => {
          val segmentId = idGenerator.nextID
          Segment.persistent(
            path = targetPaths.next.resolve(IDGenerator.segmentId(segmentId)),
            segmentId = segmentId,
            createdInLevel = createdInLevel,
            segmentConfig = segmentConfig,
            mmapReads = mmapReads,
            mmapWrites = mmapWrites,
            keyValues = keyValues
          )
        },

      recover =
        (segments: Slice[Segment], _: Throwable) =>
          segments foreach {
            segmentToDelete =>
              segmentToDelete.delete onLeftSideEffect {
                exception =>
                  logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed refresh", path, segmentToDelete.path, exception)
              }
          }
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

  def get(key: Slice[Byte]): Option[Persistent] =
    segmentCache get key

  def lower(key: Slice[Byte]): Option[Persistent] =
    segmentCache lower key

  def floorHigherHint(key: Slice[Byte]): Option[Slice[Byte]] =
    segmentCache floorHigherHint key

  def higher(key: Slice[Byte]): Option[Persistent] =
    segmentCache higher key

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): Slice[KeyValue.ReadOnly] =
    segmentCache getAll addTo

  override def hasRange: Boolean =
    segmentCache.hasRange

  override def hasPut: Boolean =
    segmentCache.hasPut

  def getKeyValueCount(): Int =
    segmentCache.getBloomFilterKeyValueCount()

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

  override def hasBloomFilter: Boolean =
    segmentCache.hasBloomFilter

  def clearCachedKeyValues(): Unit =
    segmentCache.clearCachedKeyValues()

  def clearAllCaches(): Unit = {
    clearCachedKeyValues()
    segmentCache.clearLocalAndBlockCache()
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

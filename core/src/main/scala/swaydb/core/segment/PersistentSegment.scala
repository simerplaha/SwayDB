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
import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, BlockedReader}
import swaydb.core.segment.format.a.block.{SegmentBlock, _}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.data.IO._
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey}

import scala.concurrent.duration.Deadline

object PersistentSegment {
  def apply(file: DBFile,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            segmentSize: Int,
            nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     keyValueLimiter: KeyValueLimiter,
                                                     fileOpenLimiter: FileLimiter,
                                                     segmentIO: SegmentIO): IO[PersistentSegment] =
    BlockRefReader(file) map {
      blockRef =>
        val segmentCache =
          SegmentCache(
            id = file.path.toString,
            maxKey = maxKey,
            minKey = minKey,
            segmentIO = segmentIO,
            unsliceKey = true,
            blockRef = blockRef
          )

        new PersistentSegment(
          file = file,
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
                                                                          keyValueLimiter: KeyValueLimiter,
                                                                          fileOpenLimiter: FileLimiter,
                                                                          segmentIO: SegmentIO) extends Segment with LazyLogging {

  def path = file.path

  def cache: ConcurrentSkipListMap[Slice[Byte], Persistent] =
    segmentCache.persistentCache

  def close: IO[Unit] =
    file.close map {
      _ =>
        segmentCache.clear()
    }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  def deleteSegmentsEventually =
    fileOpenLimiter.delete(this)

  def delete: IO[Unit] = {
    logger.trace(s"{}: DELETING FILE", path)
    file.delete() onFailureSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure)
    } map {
      _ =>
        segmentCache.clear()
    }
  }

  def copyTo(toPath: Path): IO[Path] =
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
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.merge(
          newKeyValues = newKeyValues,
          oldKeyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          isLastLevel = removeDeletes,
          forInMemory = false,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentIO = segmentIO
        ) flatMap {
          splits =>
            splits.mapIO(
              block =
                keyValues =>
                  Segment.persistent(
                    path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                    segmentConfig = segmentConfig,
                    createdInLevel = createdInLevel,
                    mmapReads = mmapReads,
                    mmapWrites = mmapWrites,
                    keyValues = keyValues
                  ),

              recover =
                (segments: Slice[Segment], _: IO.Failure[Slice[Segment]]) =>
                  segments foreach {
                    segmentToDelete =>
                      segmentToDelete.delete onFailureSideEffect {
                        exception =>
                          logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                      }
                  }
            )
        }
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
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                          groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.split(
          keyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          isLastLevel = removeDeletes,
          forInMemory = false,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentIO = segmentIO
        ) flatMap {
          splits =>
            splits.mapIO(
              block =
                keyValues =>
                  Segment.persistent(
                    path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                    createdInLevel = createdInLevel,
                    segmentConfig = segmentConfig,
                    mmapReads = mmapReads,
                    mmapWrites = mmapWrites,
                    keyValues = keyValues
                  ),

              recover =
                (segments: Slice[Segment], _: IO.Failure[Slice[Segment]]) =>
                  segments foreach {
                    segmentToDelete =>
                      segmentToDelete.delete onFailureSideEffect {
                        exception =>
                          logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed refresh", path, segmentToDelete.path, exception)
                      }
                  }
            )
        }
    }

  def getSegmentBlockOffset(): IO[SegmentBlock.Offset] =
    file.fileSize map (fileSize => SegmentBlock.Offset(0, fileSize.toInt))

  //  def getSegmentBlock() =
  //    for {
  //      offset <- getSegmentBlockOffset()
  //      block <- SegmentBlock.readBlocked(offset, Reader(file))
  //    } yield block

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    segmentCache getFromCache key

  def mightContainKey(key: Slice[Byte]): IO[Boolean] =
    segmentCache mightContain key

  override def mightContainFunction(key: Slice[Byte]): IO[Boolean] =
    IO {
      minMaxFunctionId.exists {
        minMaxFunctionId =>
          MinMax.contains(
            key = key,
            minMax = minMaxFunctionId
          )(FunctionStore.order)
      }
    }

  def get(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentCache get key

  def lower(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentCache lower key

  def floorHigherHint(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    segmentCache floorHigherHint key

  def higher(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentCache higher key

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    segmentCache getAll addTo

  override def hasRange: IO[Boolean] =
    segmentCache.hasRange

  override def hasPut: IO[Boolean] =
    segmentCache.hasPut

  def getHeadKeyValueCount(): IO[Int] =
    segmentCache.getHeadKeyValueCount()

  def getBloomFilterKeyValueCount(): IO[Int] =
    segmentCache.getBloomFilterKeyValueCount()

  override def isFooterDefined: Boolean =
    segmentCache.isFooterDefined

  def existsOnDisk: Boolean =
    file.existsOnDisk

  def memory: Boolean =
    file.memory

  def persistent: Boolean =
    file.persistent

  def existsInMemory: Boolean =
    file.existsInMemory

  def notExistsOnDisk: Boolean =
    !file.existsOnDisk

  override def createdInLevel: IO[Int] =
    segmentCache.createdInLevel

  override def isGrouped: IO[Boolean] =
    segmentCache.isGrouped

  override def isBloomFilterDefined: Boolean =
    segmentCache.isBloomFilterDefined

  def clearCache(): Unit =
    segmentCache.clear()

  def isInCache(key: Slice[Byte]): Boolean =
    segmentCache isInCache key

  def isCacheEmpty: Boolean =
    segmentCache.isCacheEmpty

  def cacheSize: Int =
    segmentCache.cacheSize
}

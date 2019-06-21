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
import swaydb.core.segment.format.a.index.HashIndex
import swaydb.core.segment.format.a.SegmentReader
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.data.IO._
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey, Reserve}

import scala.concurrent.Future
import scala.concurrent.duration.Deadline

private[segment] case class PersistentSegment(file: DBFile,
                                              mmapReads: Boolean,
                                              mmapWrites: Boolean,
                                              minKey: Slice[Byte],
                                              maxKey: MaxKey[Slice[Byte]],
                                              segmentSize: Int,
                                              nearestExpiryDeadline: Option[Deadline],
                                              compactionReserve: Reserve[Unit])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                                functionStore: FunctionStore,
                                                                                keyValueLimiter: KeyValueLimiter,
                                                                                fileOpenLimiter: FileLimiter) extends Segment with LazyLogging {

  def path = file.path

  private[segment] val cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder)

  private val segmentManager =
    new SegmentManager(
      id = file.path.toString,
      maxKey = maxKey,
      minKey = minKey,
      cache = cache,
      unsliceKey = true,
      createReader = () => IO.Success(Reader(file))
    )

  def close: IO[Unit] =
    file.close map {
      _ =>
        segmentManager.close()
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
        segmentManager.close()
    }
  }

  override def reserveForCompactionOrGet(): Option[Unit] =
    Reserve.setBusyOrGet((), compactionReserve)

  override def freeFromCompaction(): Unit =
    Reserve.setFree(compactionReserve)

  override def isReserved: Boolean =
    compactionReserve.isBusy

  override def onRelease: Future[Unit] =
    Reserve.future(compactionReserve)

  def copyTo(toPath: Path): IO[Path] =
    file copyTo toPath

  /**
    * Default targetPath is set to this [[PersistentSegment]]'s parent directory.
    */
  def put(newKeyValues: Slice[KeyValue.ReadOnly],
          minSegmentSize: Long,
          bloomFilterFalsePositiveRate: Double,
          resetPrefixCompressionEvery: Int,
          minimumNumberOfKeyForHashIndex: Int,
          hashIndexCompensation: Int => Int,
          compressDuplicateValues: Boolean,
          removeDeletes: Boolean,
          createdInLevel: Int,
          maxProbe: Int,
          enableBinarySearchIndex: Boolean,
          buildFullBinarySearchIndex: Boolean,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.merge(
          newKeyValues = newKeyValues,
          oldKeyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          maxProbe = maxProbe,
          enableBinarySearchIndex = enableBinarySearchIndex,
          buildFullBinarySearchIndex = buildFullBinarySearchIndex,
          isLastLevel = removeDeletes,
          forInMemory = false,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          resetPrefixCompressionEvery = resetPrefixCompressionEvery,
          minimumNumberOfKeyForHashIndex = minimumNumberOfKeyForHashIndex,
          hashIndexCompensation = hashIndexCompensation,
          compressDuplicateValues = compressDuplicateValues
        ) flatMap {
          splits =>
            splits.mapIO(
              block =
                keyValues =>
                  Segment.persistent(
                    path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                    createdInLevel = createdInLevel,
                    maxProbe = maxProbe,
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
              bloomFilterFalsePositiveRate: Double,
              resetPrefixCompressionEvery: Int,
              minimumNumberOfKeyForHashIndex: Int,
              hashIndexCompensation: Int => Int,
              compressDuplicateValues: Boolean,
              removeDeletes: Boolean,
              createdInLevel: Int,
              maxProbe: Int,
              enableBinarySearchIndex: Boolean,
              buildFullBinarySearchIndex: Boolean,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                          groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.split(
          keyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          isLastLevel = removeDeletes,
          forInMemory = false,
          maxProbe = maxProbe,
          enableBinarySearchIndex = enableBinarySearchIndex,
          buildFullBinarySearchIndex = buildFullBinarySearchIndex,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          resetPrefixCompressionEvery = resetPrefixCompressionEvery,
          minimumNumberOfKeyForHashIndex = minimumNumberOfKeyForHashIndex,
          hashIndexCompensation = hashIndexCompensation,
          compressDuplicateValues = compressDuplicateValues
        ) flatMap {
          splits =>
            splits.mapIO(
              block =
                keyValues =>
                  Segment.persistent(
                    path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                    createdInLevel = createdInLevel,
                    maxProbe = maxProbe,
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

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    segmentManager getFromCache key

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    segmentManager mightContain key

  def get(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentManager get key

  def lower(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentManager lower key

  def floorHigherHint(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
    segmentManager floorHigherHint key

  def higher(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    segmentManager higher key

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    segmentManager getAll addTo

  override def hasRange: IO[Boolean] =
    segmentManager.hasRange

  override def hasPut: IO[Boolean] =
    segmentManager.hasPut

  def getHeadKeyValueCount(): IO[Int] =
    segmentManager.getHeadKeyValueCount()

  def getBloomFilterKeyValueCount(): IO[Int] =
    segmentManager.getBloomFilterKeyValueCount()

  override def isFooterDefined: Boolean =
    ???

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
    segmentManager.getFooter().map(_.createdInLevel)

  override def isGrouped: IO[Boolean] =
    segmentManager.getFooter().map(_.hasGroup)
}

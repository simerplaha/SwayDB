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

import swaydb.core.util.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.DBFile
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.format.a.{SegmentFooter, SegmentReader}
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
                                              removeDeletes: Boolean,
                                              nearestExpiryDeadline: Option[Deadline],
                                              compactionReserve: Reserve[Unit])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                                functionStore: FunctionStore,
                                                                                keyValueLimiter: KeyValueLimiter,
                                                                                fileOpenLimiter: FileLimiter,
                                                                                grouping: Option[KeyValueGroupingStrategyInternal]) extends Segment with LazyLogging {

  def path = file.path

  private[segment] val cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder)

  @volatile private[segment] var footer = Option.empty[SegmentFooter]

  val segmentCache =
    new SegmentCache(
      id = file.path.toString,
      maxKey = maxKey,
      minKey = minKey,
      cache = cache,
      unsliceKey = true,
      getFooter = getFooter _,
      createReader = () => IO(createReader())
    )

  def close: IO[Unit] =
    file.close map {
      _ =>
        footer = None
    }

  def isOpen: Boolean =
    file.isOpen

  def isFileDefined =
    file.isFileDefined

  private def createReader(): Reader =
    Reader(file)

  def deleteSegmentsEventually =
    fileOpenLimiter.delete(this)

  def delete: IO[Unit] = {
    logger.trace(s"{}: DELETING FILE", path)
    file.delete() onFailureSideEffect {
      failure =>
        logger.error(s"{}: Failed to delete Segment file.", path, failure)
    } map {
      _ =>
        footer = None
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
          compressDuplicateValues: Boolean,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.merge(
          newKeyValues = newKeyValues,
          oldKeyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          forInMemory = false,
          isLastLevel = removeDeletes,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues
        ) flatMap {
          splits =>
            getFooter() flatMap {
              footer =>
                splits.mapIO(
                  ioBlock =
                    keyValues =>
                      Segment.persistent(
                        path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                        createdInLevel = footer.createdInLevel,
                        mmapReads = mmapReads,
                        mmapWrites = mmapWrites,
                        keyValues = keyValues,
                        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                        removeDeletes = removeDeletes
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
    }

  def refresh(minSegmentSize: Long,
              bloomFilterFalsePositiveRate: Double,
              compressDuplicateValues: Boolean,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): IO[Slice[Segment]] =
    getAll() flatMap {
      currentKeyValues =>
        SegmentMerger.split(
          keyValues = currentKeyValues,
          minSegmentSize = minSegmentSize,
          forInMemory = false,
          isLastLevel = removeDeletes,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues
        ) flatMap {
          splits =>
            getFooter() flatMap {
              footer =>
                splits.mapIO(
                  ioBlock =
                    keyValues =>
                      Segment.persistent(
                        path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                        createdInLevel = footer.createdInLevel,
                        mmapReads = mmapReads,
                        mmapWrites = mmapWrites,
                        keyValues = keyValues,
                        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                        removeDeletes = removeDeletes
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
    }

  def getFooter(): IO[SegmentFooter] =
    footer.map(IO.Success(_)) getOrElse {
      SegmentReader.readFooter(createReader()) map {
        segmentFooter =>
          footer = Some(segmentFooter)
          segmentFooter
      }
    }

  override def getBloomFilter: IO[Option[BloomFilter]] =
    segmentCache.getBloomFilter

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    segmentCache getFromCache key

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    segmentCache mightContain key

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
    footer.isDefined

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
    getFooter().map(_.createdInLevel)

  override def isGrouped: IO[Boolean] =
    getFooter().map(_.isGrouped)
}

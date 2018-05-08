/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import java.nio.file.{NoSuchFileException, Path}
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Consumer

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data._
import swaydb.core.level.PathsDistributor
import swaydb.core.util.TryUtil._
import swaydb.core.util._
import swaydb.data.segment.MaxKey
import swaydb.data.segment.MaxKey.{Fixed, Range}
import swaydb.data.slice.Slice
import PipeOps._

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}

private[segment] class MemorySegment(val path: Path,
                                     val minKey: Slice[Byte],
                                     val maxKey: MaxKey,
                                     val segmentSize: Int,
                                     val removeDeletes: Boolean,
                                     val _hasRange: Boolean,
                                     private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], Memory],
                                     val bloomFilter: Option[BloomFilter[Slice[Byte]]],
                                     val nearestExpiryDeadline: Option[Deadline])(implicit ordering: Ordering[Slice[Byte]]) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import ordering._

  override def put(newKeyValues: Slice[KeyValue.ReadOnly],
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double,
                   graceTimeout: FiniteDuration,
                   targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): Try[Slice[Segment]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.merge(
            newKeyValues = newKeyValues,
            oldKeyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            forInMemory = true,
            isLastLevel = removeDeletes,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            hasTimeLeftAtLeast = graceTimeout
          ) flatMap {
            splits =>
              splits.tryMap[Segment](
                tryBlock =
                  keyValues => {
                    Segment.memory(
                      path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                      keyValues = keyValues,
                      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                      removeDeletes = removeDeletes
                    )
                  },

                recover =
                  (segments: Slice[Segment], _: Failure[Iterable[Segment]]) =>
                    segments foreach {
                      segmentToDelete =>
                        segmentToDelete.delete.failed foreach {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                        }
                    }
              )
          }
      }

  override def refresh(minSegmentSize: Long,
                       bloomFilterFalsePositiveRate: Double,
                       targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): Try[Slice[Segment]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.split(
            keyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            forInMemory = true,
            isLastLevel = removeDeletes,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
          ) ==> {
            splits =>
              splits.tryMap[Segment](
                tryBlock =
                  keyValues => {
                    Segment.memory(
                      path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                      keyValues = keyValues,
                      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                      removeDeletes = removeDeletes
                    )
                  },

                recover =
                  (segments: Slice[Segment], _: Failure[Iterable[Segment]]) =>
                    segments foreach {
                      segmentToDelete =>
                        segmentToDelete.delete.failed foreach {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                        }
                    }
              )
          }
      }

  override def copyTo(toPath: Path): Try[Path] =
    Failure(SegmentException.CannotCopyInMemoryFiles(path))

  override def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly] =
    Option(cache.get(key))

  override def get(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else if (!_hasRange && !bloomFilter.forall(_.mightContain(key)))
      TryUtil.successNone
    else
      maxKey match {
        case Fixed(maxKey) if key > maxKey =>
          TryUtil.successNone

        case range: Range if key >= range.maxKey =>
          TryUtil.successNone

        case _ =>
          if (_hasRange)
            Option(cache.floorEntry(key)).map(_.getValue) match {
              case floorRange @ Some(range: Memory.Range) if key >= range.fromKey && key < range.toKey =>
                Success(floorRange)

              case _ =>
                Try(Option(cache.get(key)))
            }
          else
            Try(Option(cache.get(key)))
      }

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    Try(bloomFilter.forall(_.mightContain(key)))

  override def lower(key: Slice[Byte]): Try[Option[Memory]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        Option(cache.lowerEntry(key)).map(_.getValue)
      }

  override def higher(key: Slice[Byte]): Try[Option[Memory]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        if (_hasRange) {
          Option(cache.floorEntry(key)).map(_.getValue) match {
            case floor @ Some(floorRange: Memory.Range) if key >= floorRange.fromKey && key < floorRange.toKey =>
              floor
            case _ =>
              Option(cache.higherEntry(key)).map(_.getValue)
          }
        } else
          Option(cache.higherEntry(key)).map(_.getValue)
      }

  override def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): Try[Slice[KeyValue.ReadOnly]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        val slice = addTo getOrElse Slice.create[KeyValue.ReadOnly](cache.size())
        cache.values() forEach {
          new Consumer[Memory] {
            override def accept(value: Memory): Unit =
              slice add value
          }
        }
        slice
      }

  override def delete: Try[Unit] = {
    //cache should not be cleared.
    logger.trace(s"{}: DELETING FILE", path)
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        deleted = true
      }
  }

  override def close: Try[Unit] =
    TryUtil.successUnit

  override def getKeyValueCount(): Try[Int] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try(cache.size())

  override def isOpen: Boolean =
    !deleted

  override def isFileDefined: Boolean =
    !deleted

  override def memory: Boolean =
    true

  override def persistent: Boolean =
    false

  override def existsOnDisk: Boolean =
    false

  override def getBloomFilter: Try[Option[BloomFilter[Slice[Byte]]]] =
    Try(bloomFilter)

  override def hasRange: Try[Boolean] =
    Try(_hasRange)

  override def isFooterDefined: Boolean =
    true

}
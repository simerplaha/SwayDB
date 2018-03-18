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

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.SegmentEntry.{PutReadOnly, RangeReadOnly, RemoveReadOnly}
import swaydb.core.data.{SegmentEntryReadOnly, _}
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.util.TryUtil._
import swaydb.core.util._
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

private[segment] class MemorySegment(val path: Path,
                                     val minKey: Slice[Byte],
                                     val maxKey: Slice[Byte],
                                     val segmentSize: Int,
                                     val removeDeletes: Boolean,
                                     private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], SegmentEntryReadOnly],
                                     val bloomFilter: BloomFilter[Slice[Byte]])(implicit ordering: Ordering[Slice[Byte]]) extends Segment with LazyLogging {

  @volatile private var deleted = false

  override def put(newKeyValues: Slice[KeyValue.WriteOnly],
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double,
                   targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator): Try[Slice[Segment]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      getAll(bloomFilterFalsePositiveRate) flatMap {
        currentKeyValues =>
          SegmentMerge.merge(
            newKeyValues = newKeyValues,
            oldKeyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            forInMemory = true,
            isLastLevel = removeDeletes,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
          ) flatMap {
            splits =>
              splits.tryMap(
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
                    segments.foreach {
                      segmentToDelete =>
                        segmentToDelete.delete.failed.foreach {
                          exception =>
                            logger.error(s"{}: Failed to delete Segment '{}' in recover due to failed put", path, segmentToDelete.path, exception)
                        }
                    }
              )
          }
      }

  override def copyTo(toPath: Path): Try[Path] =
    Failure(SegmentException.CannotCopyInMemoryFiles(path))

  override def getFromCache(key: Slice[Byte]): Option[SegmentEntryReadOnly] =
    Option(cache.get(key))

  override def get(key: Slice[Byte]): Try[Option[SegmentEntryReadOnly]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else if (!bloomFilter.mightContain(key))
      Success(None)
    else
      Try(Option(cache.get(key)))

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    Try(bloomFilter mightContain key)

  override def lower(key: Slice[Byte]): Try[Option[SegmentEntryReadOnly]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        Option(cache.lowerEntry(key)).map(_.getValue)
      }

  override def higher(key: Slice[Byte]): Try[Option[SegmentEntryReadOnly]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Try {
        Option(cache.higherEntry(key)).map(_.getValue)
      }

  override def getAll(bloomFilterFalsePositiveRate: Double, addTo: Option[Slice[SegmentEntry]]): Try[Slice[SegmentEntry]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      cache.asScala.tryFoldLeft(addTo.getOrElse(Slice.create[SegmentEntry](cache.size()))) {
        case (entries, (key: Slice[Byte], value: SegmentEntryReadOnly)) =>
          value match {
            case _: PutReadOnly =>
              value.getOrFetchValue map {
                valueOption =>
                  val (reader, valueSize) =
                    valueOption.map {
                      value =>
                        (Reader(value.unslice()), value.size)
                    } getOrElse
                      (Reader.emptyReader, 0)

                  entries add SegmentEntry.Put(key, reader, valueSize, 0x00, 0x00, 0x00, bloomFilterFalsePositiveRate, entries.lastOption)
              } map {
                _ =>
                  entries
              }
            case _: RemoveReadOnly =>
              entries add SegmentEntry.Remove(key, 0x00, 0x00, bloomFilterFalsePositiveRate, entries.lastOption)
              Success(entries)

            case range: RangeReadOnly =>
              value.getOrFetchValue map {
                valueOption =>
                  val (reader, valueSize) =
                    valueOption.map {
                      value =>
                        (Reader(value.unslice()), value.size)
                    } getOrElse
                      (Reader.emptyReader, 0)
                  entries add SegmentEntry.Range(range.id, range.fromKey, range.toKey, valueSize, reader, 0x00, 0x00, 0x00, bloomFilterFalsePositiveRate, entries.lastOption)
              } map {
                _ =>
                  entries
              }
          }
      }

  override def delete: Try[Unit] = {
    logger.trace(s"{}: DELETING FILE", path)
    deleted = true
    Try(clearCache())
  }

  override def close: Try[Unit] =
    TryUtil.successUnit

  override def getKeyValueCount(): Try[Int] =
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

  override def getBloomFilter: Try[BloomFilter[Slice[Byte]]] =
    Try(bloomFilter)
}
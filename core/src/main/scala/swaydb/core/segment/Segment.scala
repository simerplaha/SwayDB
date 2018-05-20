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

import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.io.file.{DBFile, IO}
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
import swaydb.core.segment.format.one.{SegmentReader, SegmentWriter}
import swaydb.core.util.BloomFilterUtil._
import swaydb.core.util.IDGenerator
import swaydb.core.util.TryUtil._
import swaydb.data.config.Dir
import swaydb.data.segment.MaxKey
import swaydb.data.segment.MaxKey.Fixed
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}
import swaydb.core.util.PipeOps._

private[core] object Segment extends LazyLogging {

  def memory(path: Path,
             keyValues: Iterable[KeyValue.WriteOnly],
             bloomFilterFalsePositiveRate: Double,
             removeDeletes: Boolean)(implicit ordering: Ordering[Slice[Byte]]): Try[Segment] = {
    if (keyValues.isEmpty) {
      Failure(new Exception("Empty key-values submitted to memory Segment."))
    } else {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](ordering)
      val bloomFilter =
        if (keyValues.last.stats.hasRemoveRange)
          None
        else
          Some(BloomFilter[Slice[Byte]](keyValues.size, bloomFilterFalsePositiveRate))

      //Note: WriteOnly key-values can be received from Persistent Segments in which case it's important that
      //all byte arrays are unsliced before writing them to Memory Segment.
      keyValues.tryFoldLeft(Option.empty[Deadline]) {
        case (deadline, keyValue) =>
          val keyUnsliced = keyValue.key.unslice()

          keyValue match {
            case remove: Transient.Remove =>
              skipList.put(
                keyUnsliced,
                Memory.Remove(keyUnsliced, remove.deadline)
              )
              bloomFilter.foreach(_ add keyUnsliced)
              Segment.getNearestDeadline(deadline, keyValue)

            case put: Transient.Put =>
              keyValue.getOrFetchValue flatMap {
                value =>
                  val unslicedValue = value.map(_.unslice())
                  unslicedValue match {
                    case Some(value) if value.nonEmpty =>
                      skipList.put(keyUnsliced, Memory.Put(key = keyUnsliced, value = value.unslice(), put.deadline))

                    case _ =>
                      skipList.put(keyUnsliced, Memory.Put(key = keyUnsliced, value = None, put.deadline))
                  }
                  bloomFilter.foreach(_ add keyUnsliced)
                  Segment.getNearestDeadline(deadline, keyValue)
              }

            case update: Transient.Update =>
              keyValue.getOrFetchValue flatMap {
                value =>
                  val unslicedValue = value.map(_.unslice())
                  unslicedValue match {
                    case Some(value) if value.nonEmpty =>
                      skipList.put(keyUnsliced, Memory.Update(key = keyUnsliced, value = value.unslice(), update.deadline))

                    case _ =>
                      skipList.put(keyUnsliced, Memory.Update(key = keyUnsliced, value = None, update.deadline))
                  }
                  bloomFilter.foreach(_ add keyUnsliced)
                  Segment.getNearestDeadline(deadline, keyValue)
              }

            case range: KeyValue.WriteOnly.Range =>
              range.fetchFromAndRangeValue flatMap {
                case (fromValue, rangeValue) =>
                  skipList.put(
                    keyUnsliced,
                    Memory.Range(
                      fromKey = keyUnsliced,
                      toKey = range.toKey.unslice(),
                      fromValue = fromValue.map(_.unslice),
                      rangeValue = rangeValue.unslice
                    )
                  )
                  bloomFilter.foreach(_ add keyUnsliced)
                  Segment.getNearestDeadline(deadline, keyValue)
              }
          }
      } match {
        case Failure(exception) =>
          Failure(exception)

        case Success(deadline) =>
          Success(
            MemorySegment(
              path = path,
              minKey = keyValues.head.key.unslice(),
              _hasRange = keyValues.last.stats.hasRange,
              maxKey =
                keyValues.last match {
                  case range: KeyValue.WriteOnly.Range =>
                    MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

                  case keyValue =>
                    MaxKey.Fixed(keyValue.key.unslice())
                },
              segmentSize = keyValues.last.stats.memorySegmentSize,
              removeDeletes = removeDeletes,
              bloomFilter = bloomFilter,
              cache = skipList,
              nearestExpiryDeadline = deadline
            )
          )
      }
    }
  }

  def persistent(path: Path,
                 bloomFilterFalsePositiveRate: Double,
                 mmapReads: Boolean,
                 mmapWrites: Boolean,
                 keyValues: Iterable[KeyValue.WriteOnly],
                 removeDeletes: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                         keyValueLimiter: (Persistent, Segment) => Unit,
                                         fileOpenLimiter: DBFile => Unit,
                                         ec: ExecutionContext): Try[Segment] =
    SegmentWriter.toSlice(keyValues, bloomFilterFalsePositiveRate) flatMap {
      case (bytes, deadline) =>
        if (bytes.isEmpty) {
          Failure(new Exception("Empty key-values submitted to persistent Segment."))
        } else {
          val writeResult =
          //if both read and writes are mmaped. Keep the file open.
            if (mmapWrites && mmapReads)
              DBFile.mmapWriteAndRead(bytes, path, fileOpenLimiter)
            //if mmapReads is false, write bytes in mmaped mode and then close and re-open for read.
            else if (mmapWrites && !mmapReads)
              DBFile.mmapWriteAndRead(bytes, path) flatMap {
                file =>
                  //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
                  //is probably not the most efficient and should be advised not to used.
                  file.close flatMap {
                    _ =>
                      DBFile.channelRead(file.path, fileOpenLimiter)
                  }
              }
            else if (!mmapWrites && mmapReads)
              DBFile.write(bytes, path) flatMap {
                path =>
                  DBFile.mmapRead(path, fileOpenLimiter)
              }
            else
              DBFile.write(bytes, path) flatMap {
                path =>
                  DBFile.channelRead(path, fileOpenLimiter)
              }

          writeResult map {
            file =>
              PersistentSegment(
                file = file,
                mmapReads = mmapReads,
                mmapWrites = mmapWrites,
                minKey = keyValues.head.key.unslice(),
                maxKey =
                  keyValues.last match {
                    case range: KeyValue.WriteOnly.Range =>
                      MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

                    case keyValue =>
                      MaxKey.Fixed(keyValue.key.unslice())
                  },
                segmentSize = keyValues.last.stats.segmentSize,
                removeDeletes = removeDeletes,
                nearestExpiryDeadline = deadline
              )
          }
        }
    }

  def copyToPersist(segment: Segment,
                    fetchNextPath: => Path,
                    mmapSegmentsOnRead: Boolean,
                    mmapSegmentsOnWrite: Boolean,
                    removeDeletes: Boolean,
                    minSegmentSize: Long,
                    bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]],
                                                          keyValueLimiter: (Persistent, Segment) => Unit,
                                                          fileOpenLimited: DBFile => Unit,
                                                          ec: ExecutionContext): Try[Slice[Segment]] =
    segment match {
      case segment: PersistentSegment =>
        val nextPath = fetchNextPath
        segment.copyTo(nextPath) flatMap {
          _ =>
            Segment(
              path = nextPath,
              mmapReads = mmapSegmentsOnRead,
              mmapWrites = mmapSegmentsOnWrite,
              minKey = segment.minKey,
              maxKey = segment.maxKey,
              segmentSize = segment.segmentSize,
              nearestExpiryDeadline = segment.nearestExpiryDeadline,
              removeDeletes = removeDeletes
            ) recoverWith {
              case exception =>
                logger.error("Failed to copyToPersist Segment {}", segment.path, exception)
                IO.deleteIfExists(nextPath).failed foreach {
                  exception =>
                    logger.error("Failed to delete copied persistent Segment {}", segment.path, exception)
                }
                Failure(exception)
            }
        } map {
          segment =>
            Slice(segment)
        }

      case memory: MemorySegment =>
        copyToPersist(
          keyValues = Slice(memory.cache.values().asScala.toArray),
          fetchNextPath = fetchNextPath,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          mmapSegmentsOnRead = mmapSegmentsOnRead,
          mmapSegmentsOnWrite = mmapSegmentsOnWrite,
          removeDeletes = removeDeletes,
          minSegmentSize = minSegmentSize
        )
    }

  def copyToPersist(keyValues: Slice[KeyValue.ReadOnly],
                    fetchNextPath: => Path,
                    mmapSegmentsOnRead: Boolean,
                    mmapSegmentsOnWrite: Boolean,
                    removeDeletes: Boolean,
                    minSegmentSize: Long,
                    bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]],
                                                          keyValueLimiter: (Persistent, Segment) => Unit,
                                                          fileOpenLimited: DBFile => Unit,
                                                          ec: ExecutionContext): Try[Slice[Segment]] = {
    val splits =
      SegmentMerger.split(
        keyValues = keyValues,
        minSegmentSize = minSegmentSize,
        isLastLevel = removeDeletes,
        forInMemory = false,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
      )

    splits.tryMap(
      tryBlock =
        keyValues => {
          Segment.persistent(
            path = fetchNextPath,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            mmapReads = mmapSegmentsOnRead,
            mmapWrites = mmapSegmentsOnWrite,
            keyValues = keyValues,
            removeDeletes = removeDeletes
          )
        },

      recover =
        (segments: Slice[Segment], _: Failure[Slice[Segment]]) =>
          segments foreach {
            segmentToDelete =>
              segmentToDelete.delete.failed foreach {
                exception =>
                  logger.error(s"Failed to delete Segment '{}' in recover due to failed copyToPersist", segmentToDelete.path, exception)
              }
          }
    )
  }

  def copyToMemory(segment: Segment,
                   fetchNextPath: => Path,
                   removeDeletes: Boolean,
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]],
                                                         ec: ExecutionContext): Try[Slice[Segment]] =
    segment.getAll() flatMap {
      keyValues =>
        copyToMemory(
          keyValues = keyValues,
          fetchNextPath = fetchNextPath,
          removeDeletes = removeDeletes,
          minSegmentSize = minSegmentSize,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
        )
    }

  def copyToMemory(keyValues: Slice[KeyValue.ReadOnly],
                   fetchNextPath: => Path,
                   removeDeletes: Boolean,
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]],
                                                         ec: ExecutionContext): Try[Slice[Segment]] =
    SegmentMerger.split(
      keyValues = keyValues,
      minSegmentSize = minSegmentSize,
      isLastLevel = removeDeletes,
      forInMemory = true,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate
    ) tryMap { //recovery not required. On failure, uncommitted Segments will be GC'd.
      keyValues =>
        Segment.memory(
          path = fetchNextPath,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          keyValues = keyValues,
          removeDeletes = removeDeletes
        )
    }

  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey,
            segmentSize: Int,
            removeDeletes: Boolean,
            nearestExpiryDeadline: Option[Deadline],
            checkExists: Boolean = true)(implicit ordering: Ordering[Slice[Byte]],
                                         keyValueLimiter: (Persistent, Segment) => Unit,
                                         fileOpenLimited: DBFile => Unit,
                                         ec: ExecutionContext): Try[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path, fileOpenLimited, checkExists)
      else
        DBFile.channelRead(path, fileOpenLimited, checkExists)

    file map {
      file =>
        new PersistentSegment(
          file = file,
          mmapReads = mmapReads,
          mmapWrites = mmapWrites,
          minKey = minKey,
          maxKey = maxKey,
          segmentSize = segmentSize,
          removeDeletes = removeDeletes,
          nearestExpiryDeadline = nearestExpiryDeadline
        )
    }
  }

  /**
    * Reads the [[PersistentSegment]] when the min, max keys & fileSize is not known.
    *
    * This function requires the Segment to be opened and read. After the Segment is successfully
    * read the file is closed.
    *
    * This function is only used for Appendix file recovery initialization.
    */
  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            removeDeletes: Boolean,
            checkExists: Boolean)(implicit ordering: Ordering[Slice[Byte]],
                                  keyValueLimiter: (Persistent, Segment) => Unit,
                                  fileOpenLimited: DBFile => Unit,
                                  ec: ExecutionContext): Try[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path, fileOpenLimited, checkExists)
      else
        DBFile.channelRead(path, fileOpenLimited, checkExists)

    file flatMap {
      file =>
        file.fileSize flatMap {
          fileSize =>
            SegmentReader.readFooter(Reader(file)) flatMap {
              footer =>
                SegmentReader.readAll(footer, Reader(file)) flatMap {
                  keyValues =>
                    //close the file
                    file.close flatMap {
                      _ =>
                        getNearestDeadline(keyValues) map {
                          nearestDeadline =>
                            new PersistentSegment(
                              file = file,
                              mmapReads = mmapReads,
                              mmapWrites = mmapWrites,
                              minKey = keyValues.head.key,
                              maxKey =
                                keyValues.last match {
                                  case fixed: KeyValue.ReadOnly.Fixed =>
                                    MaxKey.Fixed(fixed.key)

                                  case range: KeyValue.ReadOnly.Range =>
                                    MaxKey.Range(range.fromKey, range.toKey)
                                },
                              segmentSize = fileSize.toInt,
                              nearestExpiryDeadline = nearestDeadline,
                              removeDeletes = removeDeletes
                            )
                        }
                    }
                }
            }
        }
    }
  }

  def belongsTo(keyValue: KeyValue,
                segment: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean = {
    import ordering._
    keyValue.key >= segment.minKey && {
      if (segment.maxKey.inclusive)
        keyValue.key <= segment.maxKey.maxKey
      else
        keyValue.key < segment.maxKey.maxKey
    }
  }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               segment: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, true), (segment.minKey, segment.maxKey.maxKey, segment.maxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               segments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, segment))

  def overlaps(map: Map[Slice[Byte], Memory],
               segments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey) =>
        Segment.overlaps(minKey, maxKey, segments)
    }

  def overlaps(segment1: Segment,
               segment2: Segment)(implicit ordering: Ordering[Slice[Byte]]): Boolean =
    Slice.intersects((segment1.minKey, segment1.maxKey.maxKey, segment1.maxKey.inclusive), (segment2.minKey, segment2.maxKey.maxKey, segment2.maxKey.inclusive))

  def partitionOverlapping(segments1: Iterable[Segment],
                           segments2: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): (Iterable[Segment], Iterable[Segment]) =
    segments1
      .partition(segmentToWrite => segments2.exists(existingSegment => Segment.overlaps(segmentToWrite, existingSegment)))

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment],
                     count: Int)(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else {
      val resultSegments = ListBuffer.empty[Segment]
      segments1 foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }
  }

  def overlaps(segments1: Iterable[Segment],
               segments2: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def intersects(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  /**
    * Pre condition: Segments should be sorted with their minKey in ascending order.
    */
  def getAllKeyValues(bloomFilterFalsePositiveRate: Double, segments: Iterable[Segment]): Try[Slice[KeyValue.ReadOnly]] =
    if (segments.isEmpty)
      Success(Slice.create[KeyValue.ReadOnly](0))
    else if (segments.size == 1)
      segments.head.getAll()
    else
      segments.tryFoldLeft(0) {
        case (total, segment) =>
          segment.getKeyValueCount().map(_ + total)
      } flatMap {
        totalKeyValues =>
          segments.tryFoldLeft(Slice.create[KeyValue.ReadOnly](totalKeyValues)) {
            case (allKeyValues, segment) =>
              segment getAll Some(allKeyValues)
          }
      }

  def deleteSegments(segments: Iterable[Segment]): Try[Int] =
    segments.tryFoldLeft(0, failFast = false) {
      case (deleteCount, segment) =>
        segment.delete map {
          _ =>
            deleteCount + 1
        }
    }

  def tempMinMaxKeyValues(segments: Iterable[Segment]): Slice[Memory] =
    segments.foldLeft(Slice.create[Memory](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.minKey, None)
        segment.maxKey match {
          case Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, None)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, None, Value.Update(maxKey))
        }
    }

  def tempMinMaxKeyValues(map: Map[Slice[Byte], Memory]): Slice[Memory] = {
    for {
      minKey <- map.headValue().map(memory => Memory.Put(memory.key, None))
      maxKey <- map.lastValue() map {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, None)
        case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
          Memory.Range(fromKey, toKey, None, Value.Update(toKey))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.create[Memory](0)

  def minMaxKey(map: Map[Slice[Byte], Memory]): Option[(Slice[Byte], Slice[Byte])] = {
    for {
      minKey <- map.headValue().map(_.key)
      maxKey <- map.lastValue() map {
        case fixed: Memory.Fixed =>
          fixed.key
        case Memory.Range(fromKey, toKey, fromValue, rangeValue) =>
          toKey
      }
    } yield
      (minKey, maxKey)
  }

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Try[Boolean] =
    if (busySegments.isEmpty)
      Success(false)
    else
      SegmentAssigner.assignMinMaxOnlyForSegments(
        inputSegments = inputSegments,
        targetSegments = appendixSegments
      ) map {
        assignments =>
          Segment.overlaps(
            segments1 = busySegments,
            segments2 = assignments
          ).nonEmpty
      }

  def overlapsWithBusySegments(map: Map[Slice[Byte], Memory],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Try[Boolean] =
    if (busySegments.isEmpty)
      Success(false)
    else {
      for {
        head <- map.headValue()
        last <- map.lastValue()
      } yield {
        {
          if (ordering.equiv(head.key, last.key))
            SegmentAssigner.assign(keyValues = Slice(head), segments = appendixSegments)
          else
            SegmentAssigner.assign(keyValues = Slice(head, last), segments = appendixSegments)
        } map {
          assignments =>
            Segment.overlaps(
              segments1 = busySegments,
              segments2 = assignments.keys
            ).nonEmpty
        }
      }
    } getOrElse Success(false)

  def getNearestDeadline(previous: Option[Deadline],
                         next: Option[Deadline]): Option[Deadline] =

    (previous, next) match {
      case (None, None) => None
      case (previous @ Some(_), None) => previous
      case (None, next @ Some(_)) => next
      case (Some(previous), Some(next)) =>
        if (previous < next)
          Some(previous)
        else
          Some(next)
    }

  def getNearestDeadline(previous: Option[Deadline],
                         keyValue: KeyValue): Try[Option[Deadline]] =
    keyValue match {
      case readOnly: KeyValue.ReadOnly.Fixed =>
        Try(getNearestDeadline(previous, readOnly.deadline))

      case writeOnly: KeyValue.WriteOnly.Fixed =>
        Try(getNearestDeadline(previous, writeOnly.deadline))

      case range: KeyValue.ReadOnly.Range =>
        range.fetchFromAndRangeValue map {
          case (Some(fromValue), rangeValue) =>
            getNearestDeadline(previous, fromValue.deadline) ==> {
              getNearestDeadline(_, rangeValue.deadline)
            }
          case (None, rangeValue) =>
            getNearestDeadline(previous, rangeValue.deadline)
        }

      case range: KeyValue.WriteOnly.Range =>
        range.fetchFromAndRangeValue map {
          case (Some(fromValue), rangeValue) =>
            getNearestDeadline(previous, fromValue.deadline) ==> {
              getNearestDeadline(_, rangeValue.deadline)
            }
          case (None, rangeValue) =>
            getNearestDeadline(previous, rangeValue.deadline)
        }
    }

  def getNearestDeadline(keyValues: Iterable[KeyValue]): Try[Option[Deadline]] =
    keyValues.tryFoldLeft(Option.empty[Deadline])(getNearestDeadline)

  def getNearestDeadlineSegment(previous: Segment,
                                next: Segment): Option[Segment] =
    (previous.nearestExpiryDeadline, next.nearestExpiryDeadline) match {
      case (None, None) => None
      case (Some(_), None) => Some(previous)
      case (None, Some(_)) => Some(next)
      case (Some(previousDeadline), Some(nextDeadline)) =>
        if (previousDeadline < nextDeadline)
          Some(previous)
        else
          Some(next)
    }

  def getNearestDeadlineSegment(segments: Iterable[Segment]): Option[Segment] =
    segments.foldLeft(Option.empty[Segment]) {
      case (previous, next) =>
        previous map {
          previous =>
            getNearestDeadlineSegment(previous, next)
        } getOrElse Some(next)
    }

}

private[core] trait Segment {
  private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], _]
  val minKey: Slice[Byte]
  val maxKey: MaxKey
  val segmentSize: Int
  val removeDeletes: Boolean
  val nearestExpiryDeadline: Option[Deadline]

  def getBloomFilter: Try[Option[BloomFilter[Slice[Byte]]]]

  def path: Path

  def put(newKeyValues: Slice[KeyValue.ReadOnly],
          minSegmentSize: Long,
          bloomFilterFalsePositiveRate: Double,
          hasTimeLeftAtLeast: FiniteDuration,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Try[Slice[Segment]]

  def refresh(minSegmentSize: Long,
              bloomFilterFalsePositiveRate: Double,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): Try[Slice[Segment]]

  def copyTo(toPath: Path): Try[Path]

  def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly]

  def mightContain(key: Slice[Byte]): Try[Boolean]

  def get(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly]]

  def lower(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly]]

  def higher(key: Slice[Byte]): Try[Option[KeyValue.ReadOnly]]

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): Try[Slice[KeyValue.ReadOnly]]

  def delete: Try[Unit]

  def close: Try[Unit]

  def getKeyValueCount(): Try[Int]

  def clearCache(): Unit =
    cache.clear()

  def removeFromCache(key: Slice[Byte]) =
    cache.remove(key)

  def isInCache(key: Slice[Byte]): Boolean =
    cache containsKey key

  def isCacheEmpty: Boolean =
    cache.isEmpty

  def cacheSize: Int =
    cache.size()

  def hasRange: Try[Boolean]

  def isFooterDefined: Boolean

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean

}

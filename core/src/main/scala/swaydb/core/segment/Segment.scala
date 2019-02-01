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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline
import swaydb.data.io.IO
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.{DBFile, EffectIO}
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.format.a.{SegmentReader, SegmentWriter}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.CollectionUtil._
import swaydb.core.util.PipeOps._
import swaydb.data.io.IO._
import swaydb.core.util.{BloomFilterUtil, IDGenerator}
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.slice.Slice

private[core] object Segment extends LazyLogging {

  /**
    * Deeply nested groups are not expected. This should not stack overflow.
    *
    */
  def writeBloomFilterAndGetNearestDeadline(group: KeyValue.WriteOnly.Group,
                                            bloomFilter: Option[BloomFilter[Slice[Byte]]],
                                            currentNearestDeadline: Option[Deadline]): IO[Option[Deadline]] =
    group.keyValues.foldLeftIO(currentNearestDeadline) {
      case (currentNearestDeadline, childGroup: KeyValue.WriteOnly.Group) =>
        Segment.getNearestDeadline(currentNearestDeadline, childGroup) ==> {
          nearestDeadline =>
            writeBloomFilterAndGetNearestDeadline(childGroup, bloomFilter, nearestDeadline)
        }

      case (currentNearestDeadline, otherKeyValue) =>
        //unslice is not required since this is just bloomFilter add and groups are stored as compressed bytes.
        IO {
          bloomFilter.foreach(_ add otherKeyValue.key)
          //nearest deadline compare with this key-value is not required here as it's already set by the Group.
          Segment.getNearestDeadline(currentNearestDeadline, otherKeyValue)
        }
    }

  def memory(path: Path,
             keyValues: Iterable[KeyValue.WriteOnly],
             bloomFilterFalsePositiveRate: Double,
             removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     timeOrder: TimeOrder[Slice[Byte]],
                                     functionStore: FunctionStore,
                                     fileLimiter: FileLimiter,
                                     groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                     keyValueLimiter: KeyValueLimiter): IO[Segment] =
    if (keyValues.isEmpty) {
      IO.Failure(new Exception("Empty key-values submitted to memory Segment."))
    } else {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](keyOrder)

      val bloomFilter =
        BloomFilterUtil.initBloomFilter(keyValues, bloomFilterFalsePositiveRate)

      def writeKeyValue(keyValue: KeyValue.WriteOnly,
                        currentNearestDeadline: Option[Deadline]): IO[Option[Deadline]] = {
        val keyUnsliced = keyValue.key.unslice()
        keyValue match {
          case group: KeyValue.WriteOnly.Group =>
            writeBloomFilterAndGetNearestDeadline(group, bloomFilter, currentNearestDeadline) map {
              currentNearestDeadline =>
                skipList.put(
                  keyUnsliced,
                  Memory.Group(
                    minKey = keyUnsliced,
                    maxKey = group.maxKey.unslice(),
                    //this deadline is group's nearest deadline and the Segment's nearest deadline.
                    deadline = group.deadline,
                    compressedKeyValues = group.compressedKeyValues.unslice(),
                    groupStartOffset = 0 //compressKeyValues are unsliced so startOffset is 0.
                  )
                )
                currentNearestDeadline
            }

          case remove: Transient.Remove =>
            skipList.put(
              keyUnsliced,
              Memory.Remove(
                key = keyUnsliced,
                deadline = remove.deadline,
                time = remove.time.unslice()
              )
            )
            bloomFilter.foreach(_ add keyUnsliced)
            Segment.getNearestDeadline(currentNearestDeadline, remove)

          case put: Transient.Put =>
            val unslicedValue = put.value.map(_.unslice())
            unslicedValue match {
              case Some(value) if value.nonEmpty =>
                skipList.put(
                  keyUnsliced,
                  Memory.Put(
                    key = keyUnsliced,
                    value = Some(value.unslice()),
                    deadline = put.deadline,
                    time = put.time.unslice()
                  )
                )

              case _ =>
                skipList.put(
                  keyUnsliced,
                  Memory.Put(
                    key = keyUnsliced,
                    value = None,
                    deadline = put.deadline,
                    time = put.time.unslice()
                  )
                )
            }
            bloomFilter.foreach(_ add keyUnsliced)
            Segment.getNearestDeadline(currentNearestDeadline, put)

          case update: Transient.Update =>
            val unslicedValue = update.value.map(_.unslice())
            unslicedValue match {
              case Some(value) if value.nonEmpty =>
                skipList.put(
                  keyUnsliced,
                  Memory.Update(
                    key = keyUnsliced,
                    value = Some(value.unslice()),
                    deadline = update.deadline,
                    time = update.time.unslice()
                  )
                )

              case _ =>
                skipList.put(
                  keyUnsliced,
                  Memory.Update(
                    key = keyUnsliced,
                    value = None,
                    deadline = update.deadline,
                    time = update.time.unslice()
                  )
                )
            }
            bloomFilter.foreach(_ add keyUnsliced)
            Segment.getNearestDeadline(currentNearestDeadline, update)

          case function: Transient.Function =>
            skipList.put(
              keyUnsliced,
              Memory.Function(
                key = keyUnsliced,
                function = function.function.unslice(),
                time = function.time.unslice()
              )
            )

            bloomFilter.foreach(_ add keyUnsliced)
            Segment.getNearestDeadline(currentNearestDeadline, function)

          case pendingApply: Transient.PendingApply =>
            skipList.put(
              keyUnsliced,
              Memory.PendingApply(
                key = keyUnsliced,
                applies = pendingApply.applies.map(_.unslice)
              )
            )

            bloomFilter.foreach(_ add keyUnsliced)
            Segment.getNearestDeadline(currentNearestDeadline, pendingApply)

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
                Segment.getNearestDeadline(currentNearestDeadline, range)
            }
        }
      }

      //Note: WriteOnly key-values can be received from Persistent Segments in which case it's important that
      //all byte arrays are unsliced before writing them to Memory Segment.
      keyValues.foldLeftIO(Option.empty[Deadline]) {
        case (deadline, keyValue) =>
          writeKeyValue(keyValue, deadline)
      } flatMap {
        nearestExpiryDeadline =>
          IO.Sync(
            MemorySegment(
              path = path,
              minKey = keyValues.head.key.unslice(),
              maxKey =
                keyValues.last match {
                  case range: KeyValue.WriteOnly.Range =>
                    MaxKey.Range(range.fromKey.unslice(), range.toKey.unslice())

                  case group: KeyValue.WriteOnly.Group =>
                    group.maxKey.unslice()

                  case keyValue: KeyValue.WriteOnly.Fixed =>
                    MaxKey.Fixed(keyValue.key.unslice())
                },
              _hasRange = keyValues.last.stats.hasRange,
              _hasPut = keyValues.last.stats.hasPut,
              _hasGroup = keyValues.last.stats.hasGroup,
              segmentSize = keyValues.last.stats.memorySegmentSize,
              removeDeletes = removeDeletes,
              bloomFilter = bloomFilter,
              cache = skipList,
              nearestExpiryDeadline = nearestExpiryDeadline
            )
          )
      }
    }

  def persistent(path: Path,
                 bloomFilterFalsePositiveRate: Double,
                 mmapReads: Boolean,
                 mmapWrites: Boolean,
                 keyValues: Iterable[KeyValue.WriteOnly],
                 removeDeletes: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore,
                                         keyValueLimiter: KeyValueLimiter,
                                         fileOpenLimiter: FileLimiter,
                                         compression: Option[KeyValueGroupingStrategyInternal],
                                         ec: ExecutionContext): IO[Segment] =
    SegmentWriter.write(keyValues, bloomFilterFalsePositiveRate) flatMap {
      case (bytes, nearestExpiryDeadline) =>
        if (bytes.isEmpty) {
          IO.Failure(new Exception("Empty key-values submitted to persistent Segment."))
        } else {
          val writeResult =
          //if both read and writes are mmaped. Keep the file open.
            if (mmapWrites && mmapReads)
              DBFile.mmapWriteAndRead(bytes, path, autoClose = true)
            //if mmapReads is false, write bytes in mmaped mode and then close and re-open for read.
            else if (mmapWrites && !mmapReads)
              DBFile.mmapWriteAndRead(bytes, path, autoClose = true) flatMap {
                file =>
                  //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
                  //is probably not the most efficient and should be advised not to used.
                  file.close flatMap {
                    _ =>
                      DBFile.channelRead(file.path, autoClose = true)
                  }
              }
            else if (!mmapWrites && mmapReads)
              DBFile.write(bytes, path) flatMap {
                path =>
                  DBFile.mmapRead(path, autoClose = true)
              }
            else
              DBFile.write(bytes, path) flatMap {
                path =>
                  DBFile.channelRead(path, autoClose = true)
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

                    case group: KeyValue.WriteOnly.Group =>
                      group.maxKey.unslice()

                    case keyValue: KeyValue.WriteOnly.Fixed =>
                      MaxKey.Fixed(keyValue.key.unslice())
                  },
                segmentSize = keyValues.last.stats.segmentSize,
                removeDeletes = removeDeletes,
                nearestExpiryDeadline = nearestExpiryDeadline
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
                    bloomFilterFalsePositiveRate: Double,
                    compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueLimiter: KeyValueLimiter,
                                                      fileOpenLimiter: FileLimiter,
                                                      compression: Option[KeyValueGroupingStrategyInternal],
                                                      ec: ExecutionContext): IO[Slice[Segment]] =
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
                EffectIO.deleteIfExists(nextPath).failed foreach {
                  exception =>
                    logger.error("Failed to delete copied persistent Segment {}", segment.path, exception)
                }
                IO.Failure(exception)
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
          minSegmentSize = minSegmentSize,
          compressDuplicateValues = compressDuplicateValues
        )
    }

  def copyToPersist(keyValues: Slice[KeyValue.ReadOnly],
                    fetchNextPath: => Path,
                    mmapSegmentsOnRead: Boolean,
                    mmapSegmentsOnWrite: Boolean,
                    removeDeletes: Boolean,
                    minSegmentSize: Long,
                    bloomFilterFalsePositiveRate: Double,
                    compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      keyValueLimiter: KeyValueLimiter,
                                                      fileOpenLimiter: FileLimiter,
                                                      compression: Option[KeyValueGroupingStrategyInternal],
                                                      ec: ExecutionContext): IO[Slice[Segment]] =
    SegmentMerger.split(
      keyValues = keyValues,
      minSegmentSize = minSegmentSize,
      isLastLevel = removeDeletes,
      forInMemory = false,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
      compressDuplicateValues = compressDuplicateValues
    ) flatMap {
      splits =>
        splits.mapIO(
          ioBlock =
            keyValues =>
              Segment.persistent(
                path = fetchNextPath,
                bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                mmapReads = mmapSegmentsOnRead,
                mmapWrites = mmapSegmentsOnWrite,
                keyValues = keyValues,
                removeDeletes = removeDeletes
              ),

          recover =
            (segments: Slice[Segment], _: IO.Failure[Slice[Segment]]) =>
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
                   bloomFilterFalsePositiveRate: Double,
                   compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     fileLimiter: FileLimiter,
                                                     groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                     keyValueLimiter: KeyValueLimiter,
                                                     ec: ExecutionContext): IO[Slice[Segment]] =
    segment.getAll() flatMap {
      keyValues =>
        copyToMemory(
          keyValues = keyValues,
          fetchNextPath = fetchNextPath,
          removeDeletes = removeDeletes,
          minSegmentSize = minSegmentSize,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues
        )
    }

  def copyToMemory(keyValues: Iterable[KeyValue.ReadOnly],
                   fetchNextPath: => Path,
                   removeDeletes: Boolean,
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double,
                   compressDuplicateValues: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     fileLimiter: FileLimiter,
                                                     groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                     keyValueLimiter: KeyValueLimiter,
                                                     ec: ExecutionContext): IO[Slice[Segment]] =
    SegmentMerger.split(
      keyValues = keyValues,
      minSegmentSize = minSegmentSize,
      isLastLevel = removeDeletes,
      forInMemory = true,
      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
      compressDuplicateValues = compressDuplicateValues
    ) flatMap { //recovery not required. On failure, uncommitted Segments will be GC'd as nothing holds references to them.
      keyValues =>
        keyValues mapIO {
          keyValues =>
            Segment.memory(
              path = fetchNextPath,
              bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
              keyValues = keyValues,
              removeDeletes = removeDeletes
            )
        }
    }

  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            segmentSize: Int,
            removeDeletes: Boolean,
            nearestExpiryDeadline: Option[Deadline],
            checkExists: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore,
                                         keyValueLimiter: KeyValueLimiter,
                                         fileOpenLimiter: FileLimiter,
                                         compression: Option[KeyValueGroupingStrategyInternal],
                                         ec: ExecutionContext): IO[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path = path, autoClose = true, checkExists = checkExists)
      else
        DBFile.channelRead(path = path, autoClose = true, checkExists = checkExists)

    file map {
      file =>
        PersistentSegment(
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
            checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  keyValueLimiter: KeyValueLimiter,
                                  fileOpenLimiter: FileLimiter,
                                  compression: Option[KeyValueGroupingStrategyInternal],
                                  ec: ExecutionContext): IO[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path = path, autoClose = false, checkExists = checkExists)
      else
        DBFile.channelRead(path = path, autoClose = false, checkExists = checkExists)

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
                            PersistentSegment(
                              file = file,
                              mmapReads = mmapReads,
                              mmapWrites = mmapWrites,
                              minKey = keyValues.head.key,
                              maxKey =
                                keyValues.last match {
                                  case fixed: KeyValue.ReadOnly.Fixed =>
                                    MaxKey.Fixed(fixed.key)

                                  case group: KeyValue.ReadOnly.Group =>
                                    group.maxKey

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
                segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
    import keyOrder._
    keyValue.key >= segment.minKey && {
      if (segment.maxKey.inclusive)
        keyValue.key <= segment.maxKey.maxKey
      else
        keyValue.key < segment.maxKey.maxKey
    }
  }

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, true), (segment.minKey, segment.maxKey.maxKey, segment.maxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, segment))

  def overlaps(map: Map[Slice[Byte], Memory.SegmentResponse],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey) =>
        Segment.overlaps(minKey, maxKey, segments)
    }

  def overlaps(segment1: Segment,
               segment2: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((segment1.minKey, segment1.maxKey.maxKey, segment1.maxKey.inclusive), (segment2.minKey, segment2.maxKey.maxKey, segment2.maxKey.inclusive))

  def partitionOverlapping(segments1: Iterable[Segment],
                           segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): (Iterable[Segment], Iterable[Segment]) =
    segments1
      .partition(segmentToWrite => segments2.exists(existingSegment => Segment.overlaps(segmentToWrite, existingSegment)))

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    nonOverlapping(segments1, segments2, segments1.size)

  def nonOverlapping(segments1: Iterable[Segment],
                     segments2: Iterable[Segment],
                     count: Int)(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] = {
    if (count == 0)
      Iterable.empty
    else {
      val resultSegments = ListBuffer.empty[Segment]
      segments1.iterator foreachBreak {
        segment1 =>
          if (!segments2.exists(segment2 => overlaps(segment1, segment2)))
            resultSegments += segment1
          resultSegments.size == count
      }
      resultSegments
    }
  }

  def overlaps(segments1: Iterable[Segment],
               segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def intersects(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  /**
    * Pre condition: Segments should be sorted with their minKey in ascending order.
    */
  def getAllKeyValues(bloomFilterFalsePositiveRate: Double, segments: Iterable[Segment]): IO[Slice[KeyValue.ReadOnly]] =
    if (segments.isEmpty)
      IO.Sync(Slice.create[KeyValue.ReadOnly](0))
    else if (segments.size == 1)
      segments.head.getAll()
    else
      segments.foldLeftIO(0) {
        case (total, segment) =>
          segment.getHeadKeyValueCount().map(_ + total)
      } flatMap {
        totalKeyValues =>
          segments.foldLeftIO(Slice.create[KeyValue.ReadOnly](totalKeyValues)) {
            case (allKeyValues, segment) =>
              segment getAll Some(allKeyValues)
          }
      }

  def deleteSegments(segments: Iterable[Segment]): IO[Int] =
    segments.foldLeftIO(0, failFast = false) {
      case (deleteCount, segment) =>
        segment.delete map {
          _ =>
            deleteCount + 1
        }
    }

  def tempMinMaxKeyValues(segments: Iterable[Segment]): Slice[Memory] =
    segments.foldLeft(Slice.create[Memory](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.minKey, None, None, Time.empty)
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, None, None, Time.empty)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, None, Value.Update(Some(maxKey), None, Time.empty))
        }
    }

  def tempMinMaxKeyValues(map: Map[Slice[Byte], Memory.SegmentResponse]): Slice[Memory] = {
    for {
      minKey <- map.headValue().map(memory => Memory.Put(memory.key, None, None, Time.empty))
      maxKey <- map.lastValue() map {
        case fixed: Memory.Fixed =>
          Memory.Put(fixed.key, None, None, Time.empty)

        case Memory.Range(fromKey, toKey, _, _) =>
          Memory.Range(fromKey, toKey, None, Value.Update(None, None, Time.empty))
      }
    } yield
      Slice(minKey, maxKey)
  } getOrElse Slice.create[Memory](0)

  def minMaxKey(map: Map[Slice[Byte], Memory.SegmentResponse]): Option[(Slice[Byte], Slice[Byte])] =
    for {
      minKey <- map.headValue().map(_.key)
      maxKey <- map.lastValue() map {
        case fixed: Memory.Fixed =>
          fixed.key

        case range: Memory.Range =>
          range.toKey
      }
    } yield {
      (minKey, maxKey)
    }

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Boolean] =
    if (busySegments.isEmpty)
      IO.Sync(false)
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

  def overlapsWithBusySegments(map: Map[Slice[Byte], Memory.SegmentResponse],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Boolean] =
    if (busySegments.isEmpty)
      IO.Sync(false)
    else {
      for {
        head <- map.headValue()
        last <- map.lastValue()
      } yield {
        {
          if (keyOrder.equiv(head.key, last.key))
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
    } getOrElse IO.Sync(false)

  /**
    * Key-values such as Groups and Ranges can contain deadlines internally.
    *
    * Groups's internal key-value can contain deadline and Range's from and range value contain deadline.
    * Be sure to extract those before checking for nearest deadline. Use other [[getNearestDeadline]]
    * functions instead that take key-value as input to fetch the correct nearest deadline.
    */
  private[segment] def getNearestDeadline(deadline: Option[Deadline],
                                          next: Option[Deadline]): Option[Deadline] =

    (deadline, next) match {
      case (None, None) => None
      case (previous @ Some(_), None) => previous
      case (None, next @ Some(_)) => next
      case (Some(previous), Some(next)) =>
        if (previous < next)
          Some(previous)
        else
          Some(next)
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: KeyValue): IO[Option[Deadline]] =
    keyValue match {
      case readOnly: KeyValue.ReadOnly =>
        getNearestDeadline(deadline, readOnly)

      case writeOnly: KeyValue.WriteOnly =>
        IO(getNearestDeadline(deadline, writeOnly))
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         next: KeyValue.ReadOnly): IO[Option[Deadline]] =
    next match {
      case readOnly: KeyValue.ReadOnly.Put =>
        IO(getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.Remove =>
        IO(getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.Update =>
        IO(getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.PendingApply =>
        IO(getNearestDeadline(deadline, readOnly.deadline))

      case _: KeyValue.ReadOnly.Function =>
        IO(deadline)

      case range: KeyValue.ReadOnly.Range =>
        range.fetchFromAndRangeValue map {
          case (Some(fromValue), rangeValue) =>
            getNearestDeadline(deadline, fromValue) ==> {
              getNearestDeadline(_, rangeValue)
            }
          case (None, rangeValue) =>
            getNearestDeadline(deadline, rangeValue)
        }

      case group: KeyValue.ReadOnly.Group =>
        IO(getNearestDeadline(deadline, group.deadline))

    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: KeyValue.WriteOnly): Option[Deadline] =
    keyValue match {
      case writeOnly: KeyValue.WriteOnly.Fixed =>
        getNearestDeadline(deadline, writeOnly.deadline)

      case range: KeyValue.WriteOnly.Range =>
        (range.fromValue, range.rangeValue) match {
          case (Some(fromValue), rangeValue) =>
            getNearestDeadline(deadline, fromValue) ==> {
              getNearestDeadline(_, rangeValue)
            }
          case (None, rangeValue) =>
            getNearestDeadline(deadline, rangeValue)
        }

      case group: KeyValue.WriteOnly.Group =>
        getNearestDeadline(deadline, group.deadline)
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: Value.FromValue): Option[Deadline] =
    keyValue match {
      case rangeValue: Value.RangeValue =>
        getNearestDeadline(deadline, rangeValue)

      case put: Value.Put =>
        getNearestDeadline(deadline, put.deadline)
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: Value.RangeValue): Option[Deadline] =
    keyValue match {
      case remove: Value.Remove =>
        getNearestDeadline(deadline, remove.deadline)
      case update: Value.Update =>
        getNearestDeadline(deadline, update.deadline)
      case _: Value.Function =>
        deadline
      case pendingApply: Value.PendingApply =>
        getNearestDeadline(deadline, pendingApply.deadline)
    }

  def getNearestDeadline(previous: Option[Deadline],
                         applies: Slice[Value.Apply]): Option[Deadline] =
    applies.foldLeft(previous) {
      case (deadline, apply) =>
        getNearestDeadline(deadline, apply)
    }

  def getNearestDeadline(keyValues: Iterable[KeyValue]): IO[Option[Deadline]] =
    keyValues.foldLeftIO(Option.empty[Deadline])(getNearestDeadline)

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
        } getOrElse {
          if (next.nearestExpiryDeadline.isDefined)
            Some(next)
          else
            None
        }
    }

}

private[core] trait Segment {
  private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], _]
  val minKey: Slice[Byte]
  val maxKey: MaxKey[Slice[Byte]]
  val segmentSize: Int
  val removeDeletes: Boolean
  val nearestExpiryDeadline: Option[Deadline]

  def getBloomFilter: IO[Option[BloomFilter[Slice[Byte]]]]

  def path: Path

  def put(newKeyValues: Slice[KeyValue.ReadOnly],
          minSegmentSize: Long,
          bloomFilterFalsePositiveRate: Double,
          compressDuplicateValues: Boolean,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): IO[Slice[Segment]]

  def refresh(minSegmentSize: Long,
              bloomFilterFalsePositiveRate: Double,
              compressDuplicateValues: Boolean,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator): IO[Slice[Segment]]

  def copyTo(toPath: Path): IO[Path]

  def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly]

  def mightContain(key: Slice[Byte]): IO[Boolean]

  def get(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def lower(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def higher(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]]

  def delete: IO[Unit]

  def deleteSegmentsEventually: Unit

  def close: IO[Unit]

  def getHeadKeyValueCount(): IO[Int]

  def getBloomFilterKeyValueCount(): IO[Int]

  def clearCache(): Unit =
    cache.clear()

  def isInCache(key: Slice[Byte]): Boolean =
    cache containsKey key

  def isCacheEmpty: Boolean =
    cache.isEmpty

  def cacheSize: Int =
    cache.size()

  def hasRange: IO[Boolean]

  def hasPut: IO[Boolean]

  def isFooterDefined: Boolean

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean
}

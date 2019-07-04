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
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.{DBFile, IOEffect}
import swaydb.core.io.reader.Reader
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
import swaydb.core.queue.{FileLimiter, FileLimiterItem, KeyValueLimiter}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.{SegmentFooter, SegmentWriter}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.CollectionUtil._
import swaydb.core.util.{FiniteDurationUtil, IDGenerator, MinMax}
import swaydb.data.IO._
import swaydb.data.config.Dir
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey, Reserve}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Deadline

private[core] object Segment extends LazyLogging {

  val emptyIterable = Iterable.empty[Segment]
  val emptyIterableIO = IO.Success(emptyIterable)

  def memory(path: Path,
             createdInLevel: Long,
             keyValues: Iterable[KeyValue.WriteOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      fileLimiter: FileLimiter,
                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                      keyValueLimiter: KeyValueLimiter): IO[Segment] =
    if (keyValues.isEmpty) {
      IO.Failure(new Exception("Empty key-values submitted to memory Segment."))
    } else {
      val skipList = new ConcurrentSkipListMap[Slice[Byte], Memory](keyOrder)

      val bloomFilterOption = BloomFilter.init(keyValues = keyValues, ???)

      def writeKeyValue(keyValue: KeyValue.WriteOnly,
                        currentNearestDeadline: Option[Deadline]): IO[Option[Deadline]] = {
        val keyUnsliced = keyValue.key.unslice()
        keyValue match {
          case group: KeyValue.WriteOnly.Group =>
            //            SegmentWriter.write(
            //              keyValue = group,
            //              hashIndex = None,
            //              bloomFilter = bloomFilterOption,
            //              binarySearchIndex = None,
            //              currentNearestDeadline = currentNearestDeadline
            //            ) map {
            //              nextNearestDeadline =>
            //                skipList.put(
            //                  keyUnsliced,
            //                  Memory.Group(
            //                    minKey = keyUnsliced,
            //                    maxKey = group.maxKey.unslice(),
            //                    //this deadline is group's nearest deadline and the Segment's nearest deadline.
            //                    deadline = group.deadline,
            //                    compressedKeyValues = group.compressedKeyValues.unslice(),
            //                    groupStartOffset = 0 //compressKeyValues are unsliced so startOffset is 0.
            //                  )
            //                )
            //                nextNearestDeadline
            //            }
            ???

          case remove: Transient.Remove =>
            skipList.put(
              keyUnsliced,
              Memory.Remove(
                key = keyUnsliced,
                deadline = remove.deadline,
                time = remove.time.unslice()
              )
            )
            bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
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
            bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
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
            bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
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

            bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
            Segment.getNearestDeadline(currentNearestDeadline, function)

          case pendingApply: Transient.PendingApply =>
            skipList.put(
              keyUnsliced,
              Memory.PendingApply(
                key = keyUnsliced,
                applies = pendingApply.applies.map(_.unslice)
              )
            )

            bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
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
                bloomFilterOption foreach (BloomFilter.add(keyUnsliced, _))
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
          bloomFilterOption
            .map {
              bloomFilterState: BloomFilter.State =>
                val unslicedBytes = bloomFilterState.bytes.unslice()
                BloomFilter
                  .read(
                    offset = BloomFilter.Offset(0, unslicedBytes.size),
                    segmentReader = Reader(unslicedBytes)
                  )
                  .map {
                    bloom =>
                      Some(bloom, unslicedBytes)
                  }
            }
            .getOrElse(IO.none)
            .map {
              bloomFilter =>
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
                  _isGrouped = groupingStrategy.isDefined,
                  _createdInLevel = createdInLevel.toInt,
                  _hasRange = keyValues.last.stats.segmentHasRange,
                  _hasPut = keyValues.last.stats.segmentHasPut,
                  _hasGroup = keyValues.last.stats.segmentHasGroup,
                  segmentSize = keyValues.last.stats.memorySegmentSize,
                  minMaxFunctionId = ???,
                  bloomFilter = bloomFilter,
                  cache = skipList,
                  nearestExpiryDeadline = nearestExpiryDeadline,
                  busy = Reserve()
                )
            }
      }
    }

  def persistent(path: Path,
                 createdInLevel: Int,
                 maxProbe: Int,
                 mmapReads: Boolean,
                 mmapWrites: Boolean,
                 blockCompressions: BlocksCompression,
                 keyValues: Iterable[KeyValue.WriteOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          keyValueLimiter: KeyValueLimiter,
                                                          fileOpenLimiter: FileLimiter): IO[Segment] =
    SegmentWriter.write(
      keyValues = keyValues,
      createdInLevel = createdInLevel,
      maxProbe = maxProbe,
      blockCompressions = blockCompressions
    ) flatMap {
      result =>
        if (result.isEmpty) {
          //This is fatal!! Empty Segments should never be created. If this does have for whatever reason it should
          //not be allowed so that whatever is creating this Segment (eg: compaction) does not progress with a success response.
          IO.Failure(IO.Error.Fatal(new Exception("Empty key-values submitted to persistent Segment.")))
        } else {
          val writeResult =
          //if both read and writes are mmaped. Keep the file open.
            if (mmapWrites && mmapReads)
              DBFile.mmapWriteAndRead(path = path, autoClose = true, result.segmentBytes)
            //if mmapReads is false, write bytes in mmaped mode and then close and re-open for read.
            else if (mmapWrites && !mmapReads)
              DBFile.mmapWriteAndRead(path = path, autoClose = true, result.segmentBytes) flatMap {
                file =>
                  //close immediately to force flush the bytes to disk. Having mmapWrites == true and mmapReads == false,
                  //is probably not the most efficient and should be advised not to used.
                  file.close flatMap {
                    _ =>
                      DBFile.channelRead(file.path, autoClose = true)
                  }
              }
            else if (!mmapWrites && mmapReads)
              DBFile.write(path, result.segmentBytes) flatMap {
                path =>
                  DBFile.mmapRead(path, autoClose = true)
              }
            else
              DBFile.write(path, result.segmentBytes) flatMap {
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
                segmentSize = result.segmentSize,
                minMaxFunctionId = result.minMaxFunctionId,
                nearestExpiryDeadline = result.nearestDeadline,
                compactionReserve = Reserve()
              )
          }
        }
    }

  def copyToPersist(segment: Segment,
                    blockCompressions: BlocksCompression,
                    createdInLevel: Int,
                    fetchNextPath: => Path,
                    mmapSegmentsOnRead: Boolean,
                    mmapSegmentsOnWrite: Boolean,
                    removeDeletes: Boolean,
                    minSegmentSize: Long,
                    valuesConfig: Values.Config,
                    sortedIndexConfig: SortedIndex.Config,
                    binarySearchIndexConfig: BinarySearchIndex.Config,
                    hashIndexConfig: HashIndex.Config,
                    bloomFilterConfig: BloomFilter.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           keyValueLimiter: KeyValueLimiter,
                                                           fileOpenLimiter: FileLimiter,
                                                           compression: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
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
              minMaxFunctionId = segment.minMaxFunctionId,
              nearestExpiryDeadline = segment.nearestExpiryDeadline
            ) onFailureSideEffect {
              exception =>
                logger.error("Failed to copyToPersist Segment {}", segment.path, exception)
                IOEffect.deleteIfExists(nextPath) onFailureSideEffect {
                  exception =>
                    logger.error("Failed to delete copied persistent Segment {}", segment.path, exception)
                }
            }
        } map {
          segment =>
            Slice(segment)
        }

      case memory: MemorySegment =>
        copyToPersist(
          keyValues = Slice(memory.cache.values().asScala.toArray),
          blockCompressions = blockCompressions,
          createdInLevel = createdInLevel,
          fetchNextPath = fetchNextPath,
          mmapSegmentsOnRead = mmapSegmentsOnRead,
          mmapSegmentsOnWrite = mmapSegmentsOnWrite,
          removeDeletes = removeDeletes,
          minSegmentSize = minSegmentSize,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig
        )
    }

  def copyToPersist(keyValues: Slice[KeyValue.ReadOnly],
                    blockCompressions: BlocksCompression,
                    createdInLevel: Int,
                    fetchNextPath: => Path,
                    mmapSegmentsOnRead: Boolean,
                    mmapSegmentsOnWrite: Boolean,
                    removeDeletes: Boolean,
                    minSegmentSize: Long,
                    valuesConfig: Values.Config,
                    sortedIndexConfig: SortedIndex.Config,
                    binarySearchIndexConfig: BinarySearchIndex.Config,
                    hashIndexConfig: HashIndex.Config,
                    bloomFilterConfig: BloomFilter.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                           functionStore: FunctionStore,
                                                           keyValueLimiter: KeyValueLimiter,
                                                           fileOpenLimiter: FileLimiter,
                                                           compression: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]] =
    SegmentMerger.split(
      keyValues = keyValues,
      minSegmentSize = minSegmentSize,
      isLastLevel = removeDeletes,
      forInMemory = false,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig
    ) flatMap {
      splits =>
        splits.mapIO(
          block =
            keyValues =>
              Segment.persistent(
                path = fetchNextPath,
                createdInLevel = createdInLevel,
                blockCompressions = blockCompressions,
                maxProbe = hashIndexConfig.maxProbe,
                mmapReads = mmapSegmentsOnRead,
                mmapWrites = mmapSegmentsOnWrite,
                keyValues = keyValues
              ),

          recover =
            (segments: Slice[Segment], _: IO.Failure[Slice[Segment]]) =>
              segments foreach {
                segmentToDelete =>
                  segmentToDelete.delete onFailureSideEffect {
                    exception =>
                      logger.error(s"Failed to delete Segment '{}' in recover due to failed copyToPersist", segmentToDelete.path, exception)
                  }
              }
        )
    }

  def copyToMemory(segment: Segment,
                   createdInLevel: Int,
                   fetchNextPath: => Path,
                   removeDeletes: Boolean,
                   minSegmentSize: Long,
                   valuesConfig: Values.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   hashIndexConfig: HashIndex.Config,
                   bloomFilterConfig: BloomFilter.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          fileLimiter: FileLimiter,
                                                          groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                          keyValueLimiter: KeyValueLimiter): IO[Slice[Segment]] =
    segment.getAll() flatMap {
      keyValues =>
        copyToMemory(
          keyValues = keyValues,
          fetchNextPath = fetchNextPath,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          minSegmentSize = minSegmentSize,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig
        )
    }

  def copyToMemory(keyValues: Iterable[KeyValue.ReadOnly],
                   fetchNextPath: => Path,
                   removeDeletes: Boolean,
                   minSegmentSize: Long,
                   createdInLevel: Long,
                   valuesConfig: Values.Config,
                   sortedIndexConfig: SortedIndex.Config,
                   binarySearchIndexConfig: BinarySearchIndex.Config,
                   hashIndexConfig: HashIndex.Config,
                   bloomFilterConfig: BloomFilter.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                          timeOrder: TimeOrder[Slice[Byte]],
                                                          functionStore: FunctionStore,
                                                          fileLimiter: FileLimiter,
                                                          groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                          keyValueLimiter: KeyValueLimiter): IO[Slice[Segment]] =
    SegmentMerger.split(
      keyValues = keyValues,
      minSegmentSize = minSegmentSize,
      isLastLevel = removeDeletes,
      forInMemory = true,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig
    ) flatMap { //recovery not required. On failure, uncommitted Segments will be GC'd as nothing holds references to them.
      keyValues =>
        keyValues mapIO {
          keyValues =>
            Segment.memory(
              path = fetchNextPath,
              createdInLevel = createdInLevel,
              keyValues = keyValues
            )
        }
    }

  def apply(path: Path,
            mmapReads: Boolean,
            mmapWrites: Boolean,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            segmentSize: Int,
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            nearestExpiryDeadline: Option[Deadline],
            checkExists: Boolean = true)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                         timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore,
                                         keyValueLimiter: KeyValueLimiter,
                                         fileOpenLimiter: FileLimiter): IO[Segment] = {

    val fileIO =
      if (mmapReads)
        DBFile.mmapRead(path = path, autoClose = true, checkExists = checkExists)
      else
        DBFile.channelRead(path = path, autoClose = true, checkExists = checkExists)

    fileIO map {
      file =>
        PersistentSegment(
          file = file,
          mmapReads = mmapReads,
          mmapWrites = mmapWrites,
          minKey = minKey,
          maxKey = maxKey,
          segmentSize = segmentSize,
          minMaxFunctionId = minMaxFunctionId,
          nearestExpiryDeadline = nearestExpiryDeadline,
          compactionReserve = Reserve()
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
            checkExists: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  keyValueLimiter: KeyValueLimiter,
                                  fileOpenLimiter: FileLimiter,
                                  compression: Option[KeyValueGroupingStrategyInternal]): IO[Segment] = {

    val file =
      if (mmapReads)
        DBFile.mmapRead(path = path, autoClose = false, checkExists = checkExists)
      else
        DBFile.channelRead(path = path, autoClose = false, checkExists = checkExists)

    file flatMap {
      file =>
        file.fileSize flatMap {
          fileSize =>
            SegmentFooter.read(Reader(file)) flatMap {
              footer =>
                //                SortedIndex
                //                  .readAll(
                //                    offset = footer.sortedIndexOffset,
                //                    keyValueCount = footer.keyValueCount,
                //                    reader = Reader(file)
                //                  )
                //                  .flatMap {
                //                    keyValues =>
                //                      //close the file
                //                      file.close flatMap {
                //                        _ =>
                //                          getNearestDeadline(keyValues) map {
                //                            nearestDeadline =>
                //                              PersistentSegment(
                //                                file = file,
                //                                mmapReads = mmapReads,
                //                                mmapWrites = mmapWrites,
                //                                minKey = keyValues.head.key,
                //                                maxKey =
                //                                  keyValues.last match {
                //                                    case fixed: KeyValue.ReadOnly.Fixed =>
                //                                      MaxKey.Fixed(fixed.key)
                //
                //                                    case group: KeyValue.ReadOnly.Group =>
                //                                      group.maxKey
                //
                //                                    case range: KeyValue.ReadOnly.Range =>
                //                                      MaxKey.Range(range.fromKey, range.toKey)
                //                                  },
                //                                segmentSize = fileSize.toInt,
                //                                nearestExpiryDeadline = nearestDeadline,
                //                                compactionReserve = Reserve()
                //                              )
                //                          }
                //                      }
                //                  }
                ???
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
               maxKeyInclusive: Boolean,
               segment: Segment)(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Slice.intersects((minKey, maxKey, maxKeyInclusive), (segment.minKey, segment.maxKey.maxKey, segment.maxKey.inclusive))

  def overlaps(minKey: Slice[Byte],
               maxKey: Slice[Byte],
               maxKeyInclusive: Boolean,
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments.exists(segment => overlaps(minKey, maxKey, maxKeyInclusive, segment))

  def overlaps(map: Map[Slice[Byte], Memory.SegmentResponse],
               segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    Segment.minMaxKey(map) exists {
      case (minKey, maxKey, maxKeyInclusive) =>
        Segment.overlaps(
          minKey = minKey,
          maxKey = maxKey,
          maxKeyInclusive = maxKeyInclusive,
          segments = segments
        )
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
               segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    segments1.filter(segment1 => segments2.exists(segment2 => overlaps(segment1, segment2)))

  def overlaps(segment: Segment,
               segments2: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean =
    segments2.exists(segment2 => overlaps(segment, segment2))

  def intersects(segments1: Iterable[Segment], segments2: Iterable[Segment]): Boolean =
    if (segments1.isEmpty || segments2.isEmpty)
      false
    else
      segments1.exists(segment1 => segments2.exists(_.path == segment1.path))

  def intersects(segment: Segment, segments2: Iterable[Segment]): Boolean =
    segments2.exists(_.path == segment.path)

  /**
    * Pre condition: Segments should be sorted with their minKey in ascending order.
    */
  def getAllKeyValues(segments: Iterable[Segment]): IO[Slice[KeyValue.ReadOnly]] =
    if (segments.isEmpty)
      IO.Success(Slice.create[KeyValue.ReadOnly](0))
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

  def tempMinMaxKeyValues(segments: Iterable[Segment]): Slice[Memory.SegmentResponse] =
    segments.foldLeft(Slice.create[Memory.SegmentResponse](segments.size * 2)) {
      case (keyValues, segment) =>
        keyValues add Memory.Put(segment.minKey, None, None, Time.empty)
        segment.maxKey match {
          case MaxKey.Fixed(maxKey) =>
            keyValues add Memory.Put(maxKey, None, None, Time.empty)

          case MaxKey.Range(fromKey, maxKey) =>
            keyValues add Memory.Range(fromKey, maxKey, None, Value.Update(Some(maxKey), None, Time.empty))
        }
    }

  def tempMinMaxKeyValues(map: Map[Slice[Byte], Memory.SegmentResponse]): Slice[Memory.SegmentResponse] = {
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
  } getOrElse Slice.create[Memory.SegmentResponse](0)

  def minMaxKey(map: Map[Slice[Byte], Memory.SegmentResponse]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- map.headValue().map(_.key)
      maxKey <- map.lastValue() map {
        case fixed: Memory.Fixed =>
          (fixed.key, true)

        case range: Memory.Range =>
          (range.toKey, false)
      }
    } yield (minKey, maxKey._1, maxKey._2)

  def minMaxKey(segment: Iterable[Segment]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    for {
      minKey <- segment.headOption.map(_.minKey)
      maxKey <- segment.lastOption.map(_.maxKey) map {
        case MaxKey.Fixed(maxKey) =>
          (maxKey, true)

        case MaxKey.Range(_, maxKey) =>
          (maxKey, false)
      }
    } yield {
      (minKey, maxKey._1, maxKey._2)
    }

  def minMaxKey(left: Iterable[Segment],
                right: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def minMaxKey(left: Iterable[Segment],
                right: Map[Slice[Byte], Memory.SegmentResponse])(implicit keyOrder: KeyOrder[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] =
    Slice.minMax(Segment.minMaxKey(left), Segment.minMaxKey(right))

  def overlapsWithBusySegments(inputSegments: Iterable[Segment],
                               busySegments: Iterable[Segment],
                               appendixSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Boolean] =
    if (busySegments.isEmpty)
      IO.`false`
    else
      SegmentAssigner.assignMinMaxOnly(
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
      IO.`false`
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
    } getOrElse IO.`false`

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
        IO(FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.Remove =>
        IO(FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.Update =>
        IO(FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline))

      case readOnly: KeyValue.ReadOnly.PendingApply =>
        IO(FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline))

      case _: KeyValue.ReadOnly.Function =>
        IO(deadline)

      case range: KeyValue.ReadOnly.Range =>
        range.fetchFromAndRangeValue map {
          case (Some(fromValue), rangeValue) =>
            val fromValueDeadline = getNearestDeadline(deadline, fromValue)
            getNearestDeadline(fromValueDeadline, rangeValue)

          case (None, rangeValue) =>
            getNearestDeadline(deadline, rangeValue)
        }

      case group: KeyValue.ReadOnly.Group =>
        IO(FiniteDurationUtil.getNearestDeadline(deadline, group.deadline))
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: KeyValue.WriteOnly): Option[Deadline] =
    keyValue match {
      case writeOnly: KeyValue.WriteOnly.Fixed =>
        FiniteDurationUtil.getNearestDeadline(deadline, writeOnly.deadline)

      case range: KeyValue.WriteOnly.Range =>
        (range.fromValue, range.rangeValue) match {
          case (Some(fromValue), rangeValue) =>
            val fromValueDeadline = getNearestDeadline(deadline, fromValue)
            getNearestDeadline(fromValueDeadline, rangeValue)

          case (None, rangeValue) =>
            getNearestDeadline(deadline, rangeValue)
        }

      case group: KeyValue.WriteOnly.Group =>
        FiniteDurationUtil.getNearestDeadline(deadline, group.deadline)
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         keyValue: Value.FromValue): Option[Deadline] =
    keyValue match {
      case rangeValue: Value.RangeValue =>
        getNearestDeadline(deadline, rangeValue)

      case put: Value.Put =>
        FiniteDurationUtil.getNearestDeadline(deadline, put.deadline)
    }

  def getNearestDeadline(deadline: Option[Deadline],
                         rangeValue: Value.RangeValue): Option[Deadline] =
    rangeValue match {
      case remove: Value.Remove =>
        FiniteDurationUtil.getNearestDeadline(deadline, remove.deadline)
      case update: Value.Update =>
        FiniteDurationUtil.getNearestDeadline(deadline, update.deadline)
      case _: Value.Function =>
        deadline
      case pendingApply: Value.PendingApply =>
        FiniteDurationUtil.getNearestDeadline(deadline, pendingApply.deadline)
    }

  def getNearestDeadline(previous: Option[Deadline],
                         applies: Slice[Value.Apply]): Option[Deadline] =
    applies.foldLeft(previous) {
      case (deadline, apply) =>
        getNearestDeadline(
          deadline = deadline,
          rangeValue = apply
        )
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

private[core] trait Segment extends FileLimiterItem {
  val minKey: Slice[Byte]
  val maxKey: MaxKey[Slice[Byte]]
  val segmentSize: Int
  val nearestExpiryDeadline: Option[Deadline]
  val minMaxFunctionId: Option[MinMax[Slice[Byte]]]

  def createdInLevel: IO[Int]

  def isGrouped: IO[Boolean]

  def path: Path

  def reserveForCompactionOrGet(): Option[Unit]

  def freeFromCompaction(): Unit

  def onRelease: Future[Unit]

  def isReserved: Boolean

  def put(newKeyValues: Slice[KeyValue.ReadOnly],
          minSegmentSize: Long,
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: Values.Config,
          sortedIndexConfig: SortedIndex.Config,
          binarySearchIndexConfig: BinarySearchIndex.Config,
          hashIndexConfig: HashIndex.Config,
          bloomFilterConfig: BloomFilter.Config,
          blockCompressions: BlocksCompression,
          targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                      groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]]

  def refresh(minSegmentSize: Long,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: Values.Config,
              sortedIndexConfig: SortedIndex.Config,
              binarySearchIndexConfig: BinarySearchIndex.Config,
              hashIndexConfig: HashIndex.Config,
              bloomFilterConfig: BloomFilter.Config,
              blockCompressions: BlocksCompression,
              targetPaths: PathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq()))(implicit idGenerator: IDGenerator,
                                                                                                          groupingStrategy: Option[KeyValueGroupingStrategyInternal]): IO[Slice[Segment]]

  def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly]

  def mightContainKey(key: Slice[Byte]): IO[Boolean]

  def mightContainFunction(key: Slice[Byte]): IO[Boolean]

  def get(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def lower(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def higher(key: Slice[Byte]): IO[Option[KeyValue.ReadOnly.SegmentResponse]]

  def floorHigherHint(key: Slice[Byte]): IO[Option[Slice[Byte]]]

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]]

  def delete: IO[Unit]

  def deleteSegmentsEventually: Unit

  def close: IO[Unit]

  def getHeadKeyValueCount(): IO[Int]

  def getBloomFilterKeyValueCount(): IO[Int]

  def clearCache(): Unit

  def isInCache(key: Slice[Byte]): Boolean

  def isCacheEmpty: Boolean

  def cacheSize: Int

  def hasRange: IO[Boolean]

  def hasPut: IO[Boolean]

  def isFooterDefined: Boolean

  def isBloomFilterDefined: Boolean

  def isOpen: Boolean

  def isFileDefined: Boolean

  def memory: Boolean

  def persistent: Boolean

  def existsOnDisk: Boolean
}

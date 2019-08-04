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
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.data.Memory.{Group, SegmentResponse}
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.level.PathsDistributor
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util._
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.JavaConverters._
import scala.concurrent.duration.Deadline


private[segment] case class MemorySegment(path: Path,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                                          segmentSize: Int,
                                          _hasRange: Boolean,
                                          _hasPut: Boolean,
                                          //only Memory Segment's need to know if there is a Group. Persistent Segments always find floor from cache when reading.
                                          _hasGroup: Boolean,
                                          _isGrouped: Boolean,
                                          _createdInLevel: Int,
                                          private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], Memory],
                                          bloomFilterReader: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
                                          nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                   functionStore: FunctionStore,
                                                                                   keyValueLimiter: KeyValueLimiter,
                                                                                   fileLimiter: FileLimiter,
                                                                                   segmentIO: SegmentIO) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  /**
   * Adds the new Group to the queue only if it is not already in the Queue.
   *
   * This function is always invoked before reading the Group itself therefore if the header is not already
   * populated, it means that this is a newly fetched/decompressed Group and should be added to the [[keyValueLimiter]].
   *
   * [[keyValueLimiter]] never removes [[Memory.Group]] key-value but instead uncompressed and re-adds them to the skipList.
   *
   */
  private def addToQueueMayBe(group: Memory.Group): Unit = {
    val groupSegment = group.segment
    //If the group is already initialised then this Group is already in the Limit queue as the queue always pre-reads the header
    if (!groupSegment.blockCache.isCached && groupSegment.isKeyValueCacheEmpty)
      keyValueLimiter.add(group, cache) //this is a new decompression, add to queue.
  }

  override def put(newKeyValues: Slice[KeyValue.ReadOnly],
                   minSegmentSize: Long,
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config,
                   targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                  groupBy: Option[GroupByInternal.KeyValues]): IO[swaydb.Error.Segment, Slice[Segment]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.merge(
            newKeyValues = newKeyValues,
            oldKeyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            isLastLevel = removeDeletes,
            forInMemory = true,
            createdInLevel = createdInLevel,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig,
            segmentIO = segmentIO
          ) flatMap {
            splits =>
              splits.mapIO[Segment](
                block =
                  keyValues =>
                    Segment.memory(
                      path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                      createdInLevel = createdInLevel,
                      keyValues = keyValues
                    ),

                recover =
                  (segments: Slice[Segment], _: IO.Failure[swaydb.Error.Segment, Iterable[Segment]]) =>
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

  override def refresh(minSegmentSize: Long,
                       removeDeletes: Boolean,
                       createdInLevel: Int,
                       valuesConfig: ValuesBlock.Config,
                       sortedIndexConfig: SortedIndexBlock.Config,
                       binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                       hashIndexConfig: HashIndexBlock.Config,
                       bloomFilterConfig: BloomFilterBlock.Config,
                       segmentConfig: SegmentBlock.Config,
                       targetPaths: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                      groupBy: Option[GroupByInternal.KeyValues]): IO[swaydb.Error.Segment, Slice[Segment]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      getAll() flatMap {
        currentKeyValues =>
          SegmentMerger.split(
            keyValues = currentKeyValues,
            minSegmentSize = minSegmentSize,
            isLastLevel = removeDeletes,
            forInMemory = true,
            createdInLevel = createdInLevel,
            valuesConfig = valuesConfig,
            sortedIndexConfig = sortedIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            hashIndexConfig = hashIndexConfig,
            bloomFilterConfig = bloomFilterConfig,
            segmentIO = segmentIO
          ) flatMap {
            splits =>
              splits.mapIO[Segment](
                block =
                  keyValues =>
                    Segment.memory(
                      path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                      createdInLevel = createdInLevel,
                      keyValues = keyValues
                    ),

                recover =
                  (segments: Slice[Segment], _: IO.Failure[swaydb.Error.Segment, Iterable[Segment]]) =>
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

  override def getFromCache(key: Slice[Byte]): Option[KeyValue.ReadOnly] =
    Option(cache.get(key))

  /**
   * Basic value does not perform floor checks on the cache which are only required if the Segment contains
   * range or groups.
   */
  private def doBasicGet(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory.SegmentResponse]] =
    Option(cache.get(key)) map {
      case response: Memory.SegmentResponse =>
        IO.Success(Some(response))

      case _: Memory.Group =>
        IO.failed("Get resulted in a Group when floorEntry should've fetched the Group instead.")
    } getOrElse {
      IO.none
    }

  override def get(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory.SegmentResponse]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      mightContainKey(key, rangeCheck = true) flatMap {
        mightContain =>
          if (mightContain)
            maxKey match {
              case MaxKey.Fixed(maxKey) if key > maxKey =>
                IO.none

              case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
                IO.none

              case _ =>
                if (_hasRange || _hasGroup)
                  Option(cache.floorEntry(key)).map(_.getValue) match {
                    case Some(range: Memory.Range) if range contains key =>
                      IO.Success(Some(range))

                    case Some(group: Memory.Group) if group contains key =>
                      addToQueueMayBe(group)
                      group.segment.get(key) flatMap {
                        case Some(persistent) =>
                          persistent.toMemoryResponseOption()

                        case None =>
                          IO.none
                      }

                    case _ =>
                      doBasicGet(key)
                  }
                else
                  doBasicGet(key)
            }
          else
            IO.none
      }

  def mightContainKey(key: Slice[Byte], rangeCheck: Boolean): IO[swaydb.Error.Segment, Boolean] =
    if (rangeCheck && (_hasGroup || _hasRange))
      IO.`true`
    else
      bloomFilterReader map {
        reader =>
          BloomFilterBlock.mightContain(
            key = key,
            reader = reader.copy()
          )
      } getOrElse IO.`true`

  def mightContainKey(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    mightContainKey(key = key, rangeCheck = false)

  override def mightContainFunction(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
    IO {
      minMaxFunctionId.exists {
        minMaxFunctionId =>
          MinMax.contains(
            key = key,
            minMax = minMaxFunctionId
          )(FunctionStore.order)
      }
    }

  override def lower(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory.SegmentResponse]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      Option(cache.lowerEntry(key)) map {
        entry =>
          entry.getValue match {
            case response: Memory.SegmentResponse =>
              IO.Success(Some(response))
            case group: Memory.Group =>
              addToQueueMayBe(group)
              group.segment.lower(key) flatMap {
                case Some(persistent) =>
                  persistent.toMemoryResponseOption()
                case None =>
                  IO.none
              }
          }
      } getOrElse {
        IO.none
      }

  /**
   * Basic value does not perform floor checks on the cache which are only required if the Segment contains
   * range or groups.
   */
  private def doBasicHigher(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory.SegmentResponse]] =
    Option(cache.higherEntry(key)).map(_.getValue) map {
      case response: Memory.SegmentResponse =>
        IO.Success(Some(response))
      case group: Memory.Group =>
        group.segment.higher(key) flatMap {
          case Some(persistent) =>
            persistent.toMemoryResponseOption()
          case None =>
            IO.none
        }
    } getOrElse {
      IO.none
    }

  def floorHigherHint(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Slice[Byte]]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      hasPut map {
        hasPut =>
          if (hasPut)
            if (key < minKey)
              Some(minKey)
            else if (key < maxKey.maxKey)
              Some(key)
            else
              None
          else
            None
      }

  override def higher(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Memory.SegmentResponse]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else if (_hasRange || _hasGroup)
      Option(cache.floorEntry(key)).map(_.getValue) map {
        case floorRange: Memory.Range if floorRange contains key =>
          IO.Success(Some(floorRange))

        case floorGroup: Memory.Group if floorGroup containsHigher key =>
          addToQueueMayBe(floorGroup)
          floorGroup.segment.higher(key) flatMap {
            case Some(persistent) =>
              persistent.toMemoryResponseOption()
            case None =>
              IO.none
          } flatMap {
            higher =>
              //Group's last key-value can be inclusive or exclusive and fromKey & toKey can be the same.
              //So it's hard to know if the Group contain higher therefore a basicHigher is required if group returns None for higher.
              if (higher.isDefined)
                IO.Success(higher)
              else
                doBasicHigher(key)
          }
        case _ =>
          doBasicHigher(key)
      } getOrElse {
        doBasicHigher(key)
      }
    else
      doBasicHigher(key)

  override def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      IO {
        val slice = addTo getOrElse Slice.create[KeyValue.ReadOnly](cache.size())
        cache.values() forEach {
          new Consumer[Memory] {
            override def accept(value: Memory): Unit =
              slice add value
          }
        }
        slice
      }

  override def delete: IO[swaydb.Error.Segment, Unit] = {
    //cache should not be cleared.
    logger.trace(s"{}: DELETING FILE", path)
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      IO.Success {
        deleted = true
      }
  }

  override val close: IO[swaydb.Error.Segment, Unit] =
    IO.unit

  override def getBloomFilterKeyValueCount(): IO[swaydb.Error.Segment, Int] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      cache.values().asScala.foldLeftIO(0) {
        case (count, keyValue) =>
          keyValue match {
            case _: SegmentResponse =>
              IO.Success(count + 1)

            case group: Group =>
              group.segment.getBloomFilterKeyValueCount() map (_ + count)
          }
      }

  override def getHeadKeyValueCount(): IO[swaydb.Error.Segment, Int] =
    if (deleted)
      IO.Failure(swaydb.Error.NoSuchFile(path))
    else
      IO.Success(cache.size())

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

  override def hasRange: IO[swaydb.Error.Segment, Boolean] =
    IO(_hasRange)

  override def hasPut: IO[swaydb.Error.Segment, Boolean] =
    IO(_hasPut)

  override def isFooterDefined: Boolean =
    !deleted

  override def deleteSegmentsEventually: Unit =
    fileLimiter.delete(this)

  override def createdInLevel: IO[swaydb.Error.Segment, Int] =
    IO(_createdInLevel)

  override def isGrouped: IO[swaydb.Error.Segment, Boolean] =
    IO(_isGrouped)

  override def hasBloomFilter: IO[swaydb.Error.Segment, Boolean] =
    IO(bloomFilterReader.isDefined)

  override def clearCachedKeyValues(): Unit =
    cache.values().asScala foreach {
      case group: Group =>
        group.segment.clearCachedKeyValues()

      case _: SegmentResponse =>
        ()
    }

  override def clearAllCaches(): Unit =
    cache.values().asScala foreach {
      case group: Group =>
        val groupSegment = group.segment
        groupSegment.clearCachedKeyValues()
        groupSegment.clearLocalAndBlockCache()

      case _: SegmentResponse =>
        ()
    }

  override def isInKeyValueCache(key: Slice[Byte]): Boolean =
    cache containsKey key

  override def isKeyValueCacheEmpty: Boolean =
    cache.isEmpty

  def areAllCachesEmpty: Boolean =
    isKeyValueCacheEmpty

  override def cachedKeyValueSize: Int =
    cache.size()
}

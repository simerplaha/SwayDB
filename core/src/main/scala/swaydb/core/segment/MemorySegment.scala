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

import java.nio.file.{NoSuchFileException, Path}
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Consumer
import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Memory.{Group, SegmentResponse}
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.PathsDistributor
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.TryUtil._
import swaydb.core.util._
import swaydb.data.slice.Slice
import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import swaydb.core.function.FunctionStore
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.MaxKey

private[segment] case class MemorySegment(path: Path,
                                          minKey: Slice[Byte],
                                          maxKey: MaxKey[Slice[Byte]],
                                          segmentSize: Int,
                                          removeDeletes: Boolean,
                                          _hasRange: Boolean,
                                          _hasPut: Boolean,
                                          //only Memory Segment's need to know if there is a Group. Persistent Segments always get floor from cache when reading.
                                          _hasGroup: Boolean,
                                          private[segment] val cache: ConcurrentSkipListMap[Slice[Byte], Memory],
                                          bloomFilter: Option[BloomFilter[Slice[Byte]]],
                                          nearestExpiryDeadline: Option[Deadline])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                                                   functionStore: FunctionStore,
                                                                                   groupingStrategy: Option[KeyValueGroupingStrategyInternal],
                                                                                   keyValueLimiter: KeyValueLimiter) extends Segment with LazyLogging {

  @volatile private var deleted = false

  import keyOrder._

  /**
    * Adds the new Group to the queue only if it is not already in the Queue.
    *
    * This function is always invoked before reading the Group itself therefore if the header is not already
    * populated, it means that this is a newly fetched/decompressed Group and should be added to the [[keyValueLimiter]].
    *
    * [[keyValueLimiter]] never removes [[Memory.Group]] key-value but instead uncompressed and re-adds them to the skipList.
    */
  private def addToQueueMayBe(group: Memory.Group): Try[Unit] =
  //if the header is already decompressed then this Group is already in the Limit queue as the queue always
  //pre-reads the header
    if (group.isHeaderDecompressed)
      TryUtil.successUnit
    else //else this is a new decompression, add to queue.
      keyValueLimiter.add(group, cache)

  override def put(newKeyValues: Slice[KeyValue.ReadOnly],
                   minSegmentSize: Long,
                   bloomFilterFalsePositiveRate: Double,
                   compressDuplicateValues: Boolean,
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
            compressDuplicateValues = compressDuplicateValues
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
                       compressDuplicateValues: Boolean,
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
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            compressDuplicateValues = compressDuplicateValues
          ) flatMap {
            splits =>
              splits.tryMap[Segment](
                tryBlock =
                  keyValues =>
                    Segment.memory(
                      path = targetPaths.next.resolve(idGenerator.nextSegmentID),
                      keyValues = keyValues,
                      bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
                      removeDeletes = removeDeletes
                    ),

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

  /**
    * Basic get does not perform floor checks on the cache which are only required if the Segment contains
    * range or groups.
    */
  private def doBasicGet(key: Slice[Byte]): Try[Option[Memory.SegmentResponse]] =
    Option(cache.get(key)) map {
      case response: Memory.SegmentResponse =>
        Success(Some(response))
      case _: Memory.Group =>
        Failure(new Exception("Get resulted in a Group when floorEntry should've fetched the Group instead."))
    } getOrElse {
      TryUtil.successNone
    }

  override def get(key: Slice[Byte]): Try[Option[Memory.SegmentResponse]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))

    else if (!_hasRange && !bloomFilter.forall(_.mightContain(key)))
      TryUtil.successNone
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          TryUtil.successNone

        case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
          TryUtil.successNone

        case _ =>
          if (_hasRange || _hasGroup)
            Option(cache.floorEntry(key)).map(_.getValue) match {
              case Some(range: Memory.Range) if range contains key =>
                Success(Some(range))

              case Some(group: Memory.Group) if group contains key =>
                addToQueueMayBe(group) flatMap {
                  _ =>
                    group.segmentCache.get(key) flatMap {
                      case Some(persistent) =>
                        persistent.toMemoryResponseOption()

                      case None =>
                        TryUtil.successNone
                    }
                }

              case _ =>
                doBasicGet(key)

            }
          else
            doBasicGet(key)
      }

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    Try(bloomFilter.forall(_.mightContain(key)))

  override def lower(key: Slice[Byte]): Try[Option[Memory.SegmentResponse]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Option(cache.lowerEntry(key)) map {
        entry =>
          entry.getValue match {
            case response: Memory.SegmentResponse =>
              Success(Some(response))
            case group: Memory.Group =>
              addToQueueMayBe(group) flatMap {
                _ =>
                  group.segmentCache.lower(key) flatMap {
                    case Some(persistent) =>
                      persistent.toMemoryResponseOption()
                    case None =>
                      TryUtil.successNone
                  }
              }
          }
      } getOrElse {
        TryUtil.successNone
      }

  /**
    * Basic get does not perform floor checks on the cache which are only required if the Segment contains
    * range or groups.
    */
  private def doBasicHigher(key: Slice[Byte]): Try[Option[Memory.SegmentResponse]] =
    Option(cache.higherEntry(key)).map(_.getValue) map {
      case response: Memory.SegmentResponse =>
        Success(Some(response))
      case group: Memory.Group =>
        group.segmentCache.higher(key) flatMap {
          case Some(persistent) =>
            persistent.toMemoryResponseOption()
          case None =>
            TryUtil.successNone
        }
    } getOrElse {
      TryUtil.successNone
    }

  override def higher(key: Slice[Byte]): Try[Option[Memory.SegmentResponse]] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else if (_hasRange || _hasGroup)
      Option(cache.floorEntry(key)).map(_.getValue) map {
        case floorRange: Memory.Range if floorRange contains key =>
          Success(Some(floorRange))

        case floorGroup: Memory.Group if floorGroup containsHigher key =>
          addToQueueMayBe(floorGroup) flatMap {
            _ =>
              floorGroup.segmentCache.higher(key) flatMap {
                case Some(persistent) =>
                  persistent.toMemoryResponseOption()
                case None =>
                  TryUtil.successNone
              } flatMap {
                higher =>
                  //Group's last key-value can be inclusive or exclusive and fromKey & toKey can be the same.
                  //So it's hard to know if the Group contain higher therefore a basicHigher is required if group returns None for higher.
                  if (higher.isDefined)
                    Success(higher)
                  else
                    doBasicHigher(key)
              }
          }
        case _ =>
          doBasicHigher(key)
      } getOrElse {
        doBasicHigher(key)
      }
    else
      doBasicHigher(key)

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

  override val close: Try[Unit] =
    TryUtil.successUnit

  override def getBloomFilterKeyValueCount(): Try[Int] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      cache.values().asScala.tryFoldLeft(0) {
        case (count, keyValue) =>
          keyValue match {
            case _: SegmentResponse =>
              Success(count + 1)
            case group: Group =>
              group.header() map (count + _.bloomFilterItemsCount)
          }
      }

  override def getHeadKeyValueCount(): Try[Int] =
    if (deleted)
      Failure(new NoSuchFileException(path.toString))
    else
      Success(cache.size())

  override def isOpen: Boolean =
    !deleted

  override def isFileDefined: Boolean =
    !deleted

  override val memory: Boolean =
    true

  override val persistent: Boolean =
    false

  override val existsOnDisk: Boolean =
    false

  override def getBloomFilter: Try[Option[BloomFilter[Slice[Byte]]]] =
    Try(bloomFilter)

  override def hasRange: Try[Boolean] =
    Try(_hasRange)

  override def hasPut: Try[Boolean] =
    Try(_hasPut)

  override def isFooterDefined: Boolean =
    !deleted

}

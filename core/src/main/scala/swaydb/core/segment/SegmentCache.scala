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

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import swaydb.core.data.{Persistent, _}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.SegmentReader._
import swaydb.core.segment.format.a.{KeyMatcher, SegmentFooter, SegmentReader}
import swaydb.core.util._
import swaydb.data.order.KeyOrder
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.slice.{Reader, Slice}

private[core] class SegmentCacheInitializer(id: String,
                                            maxKey: MaxKey[Slice[Byte]],
                                            minKey: Slice[Byte],
                                            unsliceKey: Boolean,
                                            getFooter: () => Try[SegmentFooter],
                                            createReader: () => Try[Reader]) {
  private val created = new AtomicBoolean(false)
  @volatile private var cache: SegmentCache = _

  @tailrec
  final def segmentCache(implicit keyOrder: KeyOrder[Slice[Byte]],
                         keyValueLimiter: KeyValueLimiter): SegmentCache =
    if (cache != null) {
      cache
    } else if (created.compareAndSet(false, true)) {
      cache =
        new SegmentCache(
          id = id,
          maxKey = maxKey,
          minKey = minKey,
          cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder),
          unsliceKey = unsliceKey,
          getFooter = getFooter,
          createReader = createReader
        )
      cache
    } else {
      segmentCache
    }
}

private[core] class SegmentCache(id: String,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 cache: ConcurrentSkipListMap[Slice[Byte], Persistent],
                                 unsliceKey: Boolean,
                                 getFooter: () => Try[SegmentFooter],
                                 createReader: () => Try[Reader])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                  keyValueLimiter: KeyValueLimiter) extends LazyLogging {

  import keyOrder._

  private def addToCache(keyValue: Persistent.SegmentResponse): Unit = {
    if (unsliceKey) keyValue.unsliceIndexBytes
    cache.put(keyValue.key, keyValue)
    keyValueLimiter.add(keyValue, cache)
  }

  private def addToCache(group: Persistent.Group): Try[Unit] = {
    if (unsliceKey) group.unsliceIndexBytes
    keyValueLimiter.add(group, cache) map {
      _ =>
        cache.put(group.key, group)
    }
  }

  private def prepareGet[T](getOperation: (SegmentFooter, Reader) => Try[T]): Try[T] =
    getFooter() flatMap {
      footer =>
        createReader() flatMap {
          reader =>
            getOperation(footer, reader)
        }
    } recoverWith {
      case ex: Exception =>
        ExceptionUtil.logFailure(s"$id: Failed to read Segment.", ex)
        Failure(ex)
    }

  def getBloomFilter: Try[Option[BloomFilter[Slice[Byte]]]] =
    getFooter().map(_.bloomFilter)

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(cache.get(key))

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    getFooter().map(_.bloomFilter.forall(_.mightContain(key)))

  def get(key: Slice[Byte]): Try[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        TryUtil.successNone

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        TryUtil.successNone

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        TryUtil.successNone

      case _ =>
        val floorValue = Option(cache.floorEntry(key)).map(_.getValue)
        floorValue match {
          case Some(floor: Persistent.SegmentResponse) if floor.key equiv key =>
            Success(Some(floor))

          //check if the key belongs to this group.
          case Some(group: Persistent.Group) if group contains key =>
            group.segmentCache.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            Success(Some(floorRange))

          case _ =>
            prepareGet {
              (footer, reader) =>
                if (!footer.hasRange && !footer.bloomFilter.forall(_.mightContain(key)))
                  TryUtil.successNone
                else
                  find(KeyMatcher.Get(key), startFrom = floorValue, reader, footer) flatMap {
                    case Some(response: Persistent.SegmentResponse) =>
                      addToCache(response)
                      Success(Some(response))

                    case Some(group: Persistent.Group) =>
                      addToCache(group) flatMap {
                        _ =>
                          group.segmentCache.get(key)

                      }

                    case None =>
                      TryUtil.successNone
                  }
            }
        }
    }

  private def satisfyLowerFromCache(key: Slice[Byte],
                                    lowerKeyValue: Persistent): Option[Persistent] =
  //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
    if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
      Some(lowerKeyValue)
    else
      Option(cache.ceilingEntry(key)).map(_.getValue) flatMap {
        ceilingKeyValue =>
          if (lowerKeyValue.nextIndexOffset == ceilingKeyValue.indexOffset)
            Some(lowerKeyValue)
          else
            None
      }

  def lower(key: Slice[Byte]): Try[Option[Persistent.SegmentResponse]] =
    if (key <= minKey)
      TryUtil.successNone
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          get(maxKey)

        case MaxKey.Range(fromKey, _) if key > fromKey =>
          get(fromKey)

        case _ =>
          val lowerKeyValue = Option(cache.lowerEntry(key)).map(_.getValue)
          val lowerFromCache = lowerKeyValue.flatMap(satisfyLowerFromCache(key, _))
          lowerFromCache map {
            case response: Persistent.SegmentResponse =>
              Success(Some(response))

            case group: Persistent.Group =>
              group.segmentCache.lower(key)
          } getOrElse {
            prepareGet {
              (footer, reader) =>
                find(KeyMatcher.Lower(key), startFrom = lowerKeyValue, reader, footer) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    Success(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group) flatMap {
                      _ =>
                        group.segmentCache.lower(key)
                    }

                  case None =>
                    TryUtil.successNone
                }
            }
          }
      }

  private def satisfyHigherFromCache(key: Slice[Byte],
                                     floorKeyValue: Persistent): Option[Persistent] =
    floorKeyValue match {
      case floorRange: Persistent.Range if floorRange contains key =>
        Some(floorRange)

      case floorGroup: Persistent.Group if floorGroup containsHigher key =>
        Some(floorGroup)

      case _ =>
        Option(cache.higherEntry(key)).map(_.getValue) flatMap {
          higherKeyValue =>
            if (floorKeyValue.nextIndexOffset == higherKeyValue.indexOffset)
              Some(higherKeyValue)
            else
              None
        }
    }

  def higher(key: Slice[Byte]): Try[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        TryUtil.successNone

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        TryUtil.successNone

      case _ =>
        val floorKeyValue = Option(cache.floorEntry(key)).map(_.getValue)
        val higherFromCache = floorKeyValue.flatMap(satisfyHigherFromCache(key, _))

        higherFromCache map {
          case response: Persistent.SegmentResponse =>
            Success(Some(response))

          case group: Persistent.Group =>
            group.segmentCache.higher(key)
        } getOrElse {
          prepareGet {
            (footer, reader) =>
              find(KeyMatcher.Higher(key), startFrom = floorKeyValue, reader, footer) flatMap {
                case Some(response: Persistent.SegmentResponse) =>
                  addToCache(response)
                  Success(Some(response))

                case Some(group: Persistent.Group) =>
                  addToCache(group) flatMap {
                    _ =>
                      group.segmentCache.higher(key)
                  }

                case None =>
                  TryUtil.successNone
              }
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): Try[Slice[KeyValue.ReadOnly]] =
    prepareGet {
      (footer, reader) =>
        SegmentReader.readAll(footer, reader, addTo) recoverWith {
          case exception =>
            logger.trace("{}: Reading index block failed. Segment file is corrupted.", id)
            Failure(exception)
        }
    }

  def getHeadKeyValueCount(): Try[Int] =
    getFooter().map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): Try[Int] =
    getFooter().map(_.bloomFilterItemsCount)

  def footer() =
    getFooter()

  def hasRange: Try[Boolean] =
    getFooter().map(_.hasRange)

  def hasPut: Try[Boolean] =
    getFooter().map(_.hasPut)

  def isCacheEmpty =
    cache.isEmpty()
}

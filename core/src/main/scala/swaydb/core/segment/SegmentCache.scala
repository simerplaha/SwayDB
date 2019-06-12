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

import swaydb.core.util.BloomFilter
import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import swaydb.core.data.{Persistent, _}
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.SegmentReader._
import swaydb.core.segment.format.a.{KeyMatcher, SegmentFooter, SegmentReader}
import swaydb.core.util._
import swaydb.data.{IO, MaxKey}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}

private[core] class SegmentCacheInitializer(id: String,
                                            maxKey: MaxKey[Slice[Byte]],
                                            minKey: Slice[Byte],
                                            unsliceKey: Boolean,
                                            getFooter: () => IO[SegmentFooter],
                                            createReader: () => IO[Reader]) {
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
                                 getFooter: () => IO[SegmentFooter],
                                 createReader: () => IO[Reader])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                 keyValueLimiter: KeyValueLimiter) extends LazyLogging {

  import keyOrder._

  /**
    * Notes for why use putIfAbsent before adding to cache:
    *
    * Sometimes file seeks will be done if the last known cached key-value's ranges are smaller than the
    * key being searched. For example: Search key is 10, but the last lower cache key-value range is 1-5.
    * here it's unknown if a lower key 7 exists without doing a file seek. This is also one of the reasons
    * reverse iterations are slower than forward. There are ways we can improve this which will eventually be implemented.
    *
    */
  private def addToCache(keyValue: Persistent.SegmentResponse): Unit = {
    if (unsliceKey) keyValue.unsliceIndexBytes
    if (cache.putIfAbsent(keyValue.key, keyValue) == null)
      keyValueLimiter.add(keyValue, cache)
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceIndexBytes
    if (cache.putIfAbsent(group.key, group) == null)
      keyValueLimiter.add(group, cache)
  }

  private def prepareGet[T](getOperation: (SegmentFooter, Reader) => IO[T]): IO[T] =
    getFooter() flatMap {
      footer =>
        createReader() flatMap {
          reader =>
            getOperation(footer, reader)
        }
    } onFailureSideEffect {
      failure: IO.Failure[_] =>
        ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
    }

  def getBloomFilter: IO[Option[BloomFilter]] =
    getFooter() map (_.bloomFilter)

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(cache.get(key))

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    getFooter().map(_.bloomFilter.forall(_.mightContain(key)))

  def get(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        IO.none

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        IO.none

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        IO.none

      case _ =>
        val floorValue = Option(cache.floorEntry(key)).map(_.getValue)
        floorValue match {
          case Some(floor: Persistent.SegmentResponse) if floor.key equiv key =>
            IO.Success(Some(floor))

          //check if the key belongs to this group.
          case Some(group: Persistent.Group) if group contains key =>
            group.segmentCache.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Success(Some(floorRange))

          case _ =>
            prepareGet {
              (footer, reader) =>
                if (!footer.bloomFilter.forall(_.mightContain(key)))
                  IO.none
                else
                  find(
                    matcher = KeyMatcher.Get(key),
                    startFrom = floorValue,
                    reader = reader,
                    checkHashIndex = true,
                    footer = footer
                  ) flatMap {
                    case Some(response: Persistent.SegmentResponse) =>
                      addToCache(response)
                      IO.Success(Some(response))

                    case Some(group: Persistent.Group) =>
                      addToCache(group)
                      group.segmentCache.get(key)

                    case None =>
                      IO.none
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
      lowerKeyValue match {
        case lowerRange: Persistent.Range if lowerRange containsLower key =>
          Some(lowerRange)

        case lowerGroup: Persistent.Group if lowerGroup containsLower key =>
          Some(lowerGroup)

        case _ =>
          Option(cache.ceilingEntry(key)).map(_.getValue) flatMap {
            ceilingKeyValue =>
              if (lowerKeyValue.nextIndexOffset == ceilingKeyValue.indexOffset)
                Some(lowerKeyValue)
              else
                None
          }
      }

  def lower(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    if (key <= minKey)
      IO.none
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
              IO.Success(Some(response))

            case group: Persistent.Group =>
              group.segmentCache.lower(key)
          } getOrElse {
            prepareGet {
              (footer, reader) =>
                find(
                  matcher = KeyMatcher.Lower(key),
                  startFrom = lowerKeyValue,
                  reader = reader,
                  footer = footer
                ) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    IO.Success(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group)
                    group.segmentCache.lower(key)

                  case None =>
                    IO.none
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

  def floorHigherHint(key: Slice[Byte]): IO[Option[Slice[Byte]]] =
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

  def higher(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        IO.none

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        IO.none

      case _ =>
        val floorKeyValue = Option(cache.floorEntry(key)).map(_.getValue)
        val higherFromCache = floorKeyValue.flatMap(satisfyHigherFromCache(key, _))

        higherFromCache map {
          case response: Persistent.SegmentResponse =>
            IO.Success(Some(response))

          case group: Persistent.Group =>
            group.segmentCache.higher(key)
        } getOrElse {
          prepareGet {
            (footer, reader) =>
              val startFrom =
                if (floorKeyValue.isDefined)
                  IO(floorKeyValue)
                else
                  get(key)

              startFrom flatMap {
                startFrom =>
                  find(
                    matcher = KeyMatcher.Higher(key),
                    startFrom = startFrom,
                    reader = reader,
                    footer = footer
                  ) flatMap {
                    case Some(response: Persistent.SegmentResponse) =>
                      addToCache(response)
                      IO.Success(Some(response))

                    case Some(group: Persistent.Group) =>
                      addToCache(group)
                      group.segmentCache.higher(key)

                    case None =>
                      IO.none
                  }
              }
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    prepareGet {
      (footer, reader) =>
        SegmentReader.readAll(footer, reader, addTo) onFailureSideEffect {
          _ =>
            logger.trace("{}: Reading index block failed. Segment file is corrupted.", id)
        }
    }

  def getHeadKeyValueCount(): IO[Int] =
    getFooter().map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): IO[Int] =
    getFooter().map(_.bloomFilterItemsCount)

  def footer() =
    getFooter()

  def hasRange: IO[Boolean] =
    getFooter().map(_.hasRange)

  def hasPut: IO[Boolean] =
    getFooter().map(_.hasPut)

  def isCacheEmpty =
    cache.isEmpty()
}

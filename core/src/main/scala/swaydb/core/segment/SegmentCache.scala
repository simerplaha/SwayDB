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

import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Persistent, _}
import swaydb.core.io.reader.BlockReader
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.{KeyMatcher, SegmentBlock, SegmentFooter, SegmentReader}
import swaydb.core.util._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey}

object SegmentCache {

  def apply(id: String,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            unsliceKey: Boolean,
            createSegmentBlockReader: () => IO[BlockReader[SegmentBlock]])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                           keyValueLimiter: KeyValueLimiter): SegmentCache =
    new SegmentCache(
      id = id,
      maxKey = maxKey,
      minKey = minKey,
      cache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder),
      unsliceKey = unsliceKey,
      blockCache =
        SegmentBlockCache(
          id = id,
          segmentBlock = createSegmentBlockReader
        )
    )
}
private[core] class SegmentCache(id: String,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 cache: ConcurrentSkipListMap[Slice[Byte], Persistent],
                                 unsliceKey: Boolean,
                                 blockCache: SegmentBlockCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
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
    if (unsliceKey) keyValue.unsliceKeys
    if (cache.putIfAbsent(keyValue.key, keyValue) == null)
      keyValueLimiter.add(keyValue, cache)
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceKeys
    if (cache.putIfAbsent(group.key, group) == null)
      keyValueLimiter.add(group, cache)
  }

  private def prepareGet[T](f: (SegmentFooter, Option[BlockReader[HashIndex]], Option[BlockReader[BinarySearchIndex]], BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.footer
      hashIndex <- blockCache.createHashIndexReader()
      binarySearchIndex <- blockCache.createBinarySearchReader()
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.valuesReader()
      result <- f(footer, hashIndex, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareGetAll[T](f: (SegmentFooter, BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.footer
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.valuesReader()
      result <- f(footer, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareIteration[T](f: (SegmentFooter, Option[BlockReader[BinarySearchIndex]], BlockReader[SortedIndex], Option[BlockReader[Values]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.footer
      binarySearchIndex <- blockCache.createBinarySearchReader()
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.valuesReader()
      result <- f(footer, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(cache.get(key))

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    for {
      bloom <- blockCache.createBloomFilterReader()
      contains <- bloom.map(BloomFilter.mightContain(key, _)) getOrElse IO.`true`
    } yield contains

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
            group.segment.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Success(Some(floorRange))

          case _ =>
            mightContain(key) flatMap {
              contains =>
                if (contains)
                  prepareGet {
                    (footer, hashIndex, binarySearchIndex, sortedIndex, values) =>
                      SegmentReader.get(
                        matcher = KeyMatcher.Get(key),
                        startFrom = floorValue,
                        hashIndex = hashIndex,
                        binarySearchIndex = binarySearchIndex,
                        sortedIndex = sortedIndex,
                        valuesReader = values,
                        hasRange = footer.hasRange
                      ) flatMap {
                        case Some(response: Persistent.SegmentResponse) =>
                          addToCache(response)
                          IO.Success(Some(response))

                        case Some(group: Persistent.Group) =>
                          addToCache(group)
                          group.segment.get(key)

                        case None =>
                          IO.none
                      }
                  }
                else
                  IO.none
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
              group.segment.lower(key)
          } getOrElse {
            prepareIteration {
              (footer, binarySearchIndex, sortedIndex, valuesReader) =>
                SegmentReader.lower(
                  matcher = KeyMatcher.Lower(key),
                  startFrom = lowerKeyValue,
                  binarySearch = binarySearchIndex,
                  sortedIndex = sortedIndex,
                  valuesReader
                ) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    IO.Success(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group)
                    group.segment.lower(key)

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
            group.segment.higher(key)
        } getOrElse {
          prepareIteration {
            (footer, binarySearchIndex, sortedIndex, values) =>
              val startFrom =
                if (floorKeyValue.isDefined)
                  IO(floorKeyValue)
                else
                  get(key)

              //              startFrom flatMap {
              //                startFrom =>
              //                  SegmentReader.higher(
              //                    matcher = KeyMatcher.Higher(key),
              //                    startFrom = startFrom,
              //                    reader = reader,
              //                    index = footer.sortedIndexOffset
              //                  ) flatMap {
              //                    case Some(response: Persistent.SegmentResponse) =>
              //                      addToCache(response)
              //                      IO.Success(Some(response))
              //
              //                    case Some(group: Persistent.Group) =>
              //                      addToCache(group)
              //                      group.segment.higher(key)
              //
              //                    case None =>
              //                      IO.none
              //                  }
              //              }
              ???
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    prepareGetAll {
      (footer, sortedIndex, values) =>
        SortedIndex
          .readAll(
            keyValueCount = footer.keyValueCount,
            sortedIndexReader = sortedIndex,
            valuesReader = values,
            addTo = addTo
          )
          .onFailureSideEffect {
            _ =>
              logger.trace("{}: Reading sorted index block failed.", id)
          }
    }

  def getHeadKeyValueCount(): IO[Int] =
    blockCache.footer.map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): IO[Int] =
    blockCache.footer.map(_.bloomFilterItemsCount)

  def hasRange: IO[Boolean] =
    blockCache.footer.map(_.hasRange)

  def hasPut: IO[Boolean] =
    blockCache.footer.map(_.hasPut)

  def isCacheEmpty =
    cache.isEmpty

  def isFooterDefined: Boolean =
    blockCache.isFooterDefined

  def isBloomFilterDefined: Boolean =
    blockCache.isBloomFilterDefined

  def createdInLevel: IO[Int] =
    blockCache.footer.map(_.createdInLevel)

  def isGrouped: IO[Boolean] =
    blockCache.footer.map(_.hasGroup)

  def isInCache(key: Slice[Byte]): Boolean =
    cache containsKey key

  def cacheSize: Int =
    cache.size()

  def clear() = {
    cache.clear()
    blockCache.clear()
  }

  def isInitialised() =
    blockCache.isOpen
}

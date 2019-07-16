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
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.ExceptionUtil
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.{IO, MaxKey}

private[core] object SegmentCache {

  def apply(id: String,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            unsliceKey: Boolean,
            segmentBlockOffset: SegmentBlock.Offset,
            rawSegmentReader: () => Reader,
            segmentIO: SegmentIO)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  keyValueLimiter: KeyValueLimiter): SegmentCache =
    new SegmentCache(
      id = id,
      maxKey = maxKey,
      minKey = minKey,
      persistentCache = new ConcurrentSkipListMap[Slice[Byte], Persistent](keyOrder),
      unsliceKey = unsliceKey,
      blockCache =
        SegmentBlockCache(
          id = id,
          segmentBlockOffset = segmentBlockOffset,
          rawSegmentReader = rawSegmentReader,
          segmentIO = segmentIO
        )
    )(keyOrder = keyOrder, keyValueLimiter = keyValueLimiter, groupIO = segmentIO)
}
private[core] class SegmentCache(id: String,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 private[segment] val persistentCache: ConcurrentSkipListMap[Slice[Byte], Persistent],
                                 unsliceKey: Boolean,
                                 val blockCache: SegmentBlockCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                    keyValueLimiter: KeyValueLimiter,
                                                                    groupIO: SegmentIO) extends LazyLogging {


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
    if (persistentCache.putIfAbsent(keyValue.key, keyValue) == null)
      keyValueLimiter.add(keyValue, persistentCache)
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceKeys
    if (persistentCache.putIfAbsent(group.key, group) == null)
      keyValueLimiter.add(group, persistentCache)
  }

  private def prepareGet[T](f: (SegmentFooterBlock, Option[UnblockedReader[HashIndexBlock]], Option[UnblockedReader[BinarySearchIndexBlock]], UnblockedReader[SortedIndexBlock], Option[UnblockedReader[ValuesBlock]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.getFooter()
      hashIndex <- blockCache.createHashIndexReader()
      binarySearchIndex <- blockCache.createBinarySearchIndexReader()
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.createValuesReader()
      result <- f(footer, hashIndex, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareGetAll[T](f: (SegmentFooterBlock, UnblockedReader[SortedIndexBlock], Option[UnblockedReader[ValuesBlock]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.getFooter()
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.createValuesReader()
      result <- f(footer, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  private def prepareIteration[T](f: (SegmentFooterBlock, Option[UnblockedReader[BinarySearchIndexBlock]], UnblockedReader[SortedIndexBlock], Option[UnblockedReader[ValuesBlock]]) => IO[T]): IO[T] = {
    for {
      footer <- blockCache.getFooter()
      binarySearchIndex <- blockCache.createBinarySearchIndexReader()
      sortedIndex <- blockCache.createSortedIndexReader()
      values <- blockCache.createValuesReader()
      result <- f(footer, binarySearchIndex, sortedIndex, values)
    } yield {
      result
    }
  } onFailureSideEffect {
    failure: IO.Failure[_] =>
      ExceptionUtil.logFailure(s"$id: Failed to read Segment.", failure)
  }

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    Option(persistentCache.get(key))

  def mightContain(key: Slice[Byte]): IO[Boolean] =
    blockCache.createBloomFilterReader() flatMap {
      bloomFilterReaderOption =>
        bloomFilterReaderOption map {
          bloomFilterReader =>
            BloomFilterBlock.mightContain(
              key = key,
              reader = bloomFilterReader
            )
        } getOrElse IO.`true`
    }

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
        val floorValue = Option(persistentCache.floorEntry(key)).map(_.getValue)
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
                      SegmentSearcher.search(
                        key = key,
                        start = floorValue,
                        end = None,
                        hashIndexReader = hashIndex,
                        binarySearchIndexReader = binarySearchIndex,
                        sortedIndexReader = sortedIndex,
                        valuesReaderReader = values,
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
          Option(persistentCache.ceilingEntry(key)).map(_.getValue) flatMap {
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
          val lowerKeyValue = Option(persistentCache.lowerEntry(key)).map(_.getValue)
          val lowerFromCache = lowerKeyValue.flatMap(satisfyLowerFromCache(key, _))
          lowerFromCache map {
            case response: Persistent.SegmentResponse =>
              IO.Success(Some(response))

            case group: Persistent.Group =>
              group.segment.lower(key)
          } getOrElse {
            prepareIteration {
              (footer, binarySearchIndex, sortedIndex, valuesReader) =>
                SegmentSearcher.searchLower(
                  key = key,
                  start = lowerKeyValue,
                  end = None,
                  binarySearchReader = binarySearchIndex,
                  sortedIndexReader = sortedIndex,
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
        Option(persistentCache.higherEntry(key)).map(_.getValue) flatMap {
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
        val floorKeyValue = Option(persistentCache.floorEntry(key)).map(_.getValue)
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

              startFrom flatMap {
                startFrom =>
                  SegmentSearcher.searchHigher(
                    key = key,
                    start = startFrom,
                    end = None,
                    binarySearchReader = binarySearchIndex,
                    sortedIndexReader = sortedIndex,
                    valuesReader = values
                  ) flatMap {
                    case Some(response: Persistent.SegmentResponse) =>
                      addToCache(response)
                      IO.Success(Some(response))

                    case Some(group: Persistent.Group) =>
                      addToCache(group)
                      group.segment.higher(key)

                    case None =>
                      IO.none
                  }
              }
          }
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    prepareGetAll {
      (footer, sortedIndex, values) =>
        SortedIndexBlock
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
    blockCache.getFooter().map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): IO[Int] =
    blockCache.getFooter().map(_.bloomFilterItemsCount)

  def hasRange: IO[Boolean] =
    blockCache.getFooter().map(_.hasRange)

  def hasPut: IO[Boolean] =
    blockCache.getFooter().map(_.hasPut)

  def isCacheEmpty =
    persistentCache.isEmpty

  def isFooterDefined: Boolean =
    blockCache.isFooterDefined

  def isBloomFilterDefined: Boolean =
    blockCache.isBloomFilterDefined

  def createdInLevel: IO[Int] =
    blockCache.getFooter().map(_.createdInLevel)

  def isGrouped: IO[Boolean] =
    blockCache.getFooter().map(_.hasGroup)

  def isInCache(key: Slice[Byte]): Boolean =
    persistentCache containsKey key

  def cacheSize: Int =
    persistentCache.size()

  def clear() = {
    persistentCache.clear()
    blockCache.clear()
  }

  def isInitialised() =
    blockCache.isCached
}

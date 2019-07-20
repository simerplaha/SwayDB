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
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.{IO, MaxKey}

private[core] object SegmentCache {

  def apply(id: String,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            unsliceKey: Boolean,
            blockRef: BlockRefReader[SegmentBlock.Offset],
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
          blockRef = blockRef,
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
    * reverse iterations are slower than forward.
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

  private def get(key: Slice[Byte],
                  start: Option[Persistent],
                  end: Option[Persistent],
                  hasRange: Boolean,
                  hashIndexSearchOnly: Boolean): IO[Option[Persistent.SegmentResponse]] =
    blockCache.createHashIndexReader() flatMap {
      hashIndexReader =>
        blockCache.createBinarySearchIndexReader() flatMap {
          binarySearchIndexReader =>
            blockCache.createSortedIndexReader() flatMap {
              sortedIndexReader =>
                blockCache.createValuesReader() flatMap {
                  valuesReader =>
                    SegmentSearcher.search(
                      key = key,
                      start = start,
                      end = end,
                      hashIndexReader = hashIndexReader,
                      binarySearchIndexReader = binarySearchIndexReader,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader,
                      hashIndexSearchOnly = hashIndexSearchOnly,
                      hasRange = hasRange
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
            }
        }
    }

  def get(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    get(key = key, hashIndexSearchOnly = false)


  private def get(key: Slice[Byte], hashIndexSearchOnly: Boolean): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        IO.none

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        IO.none

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        IO.none

      case _ =>
        Option(persistentCache.floorEntry(key)).map(_.getValue) match {
          case Some(floor: Persistent.SegmentResponse) if floor.key equiv key =>
            IO.Success(Some(floor))

          //check if the key belongs to this group.
          case Some(group: Persistent.Group) if group contains key =>
            group.segment.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Success(Some(floorRange))

          case floorValue =>
            blockCache.getFooter() flatMap {
              footer =>
                //if there is no hashIndex help binarySearch by sending it a higher entry.
                def getHigherForBinarySearch() =
                  if (!hashIndexSearchOnly && footer.hashIndexOffset.isEmpty && footer.binarySearchIndexOffset.isDefined)
                    Option(persistentCache.higherEntry(key)).map(_.getValue)
                  else
                    None

                if (footer.hasRange)
                  get(
                    key = key,
                    start = floorValue,
                    end = getHigherForBinarySearch(),
                    hasRange = footer.hasRange,
                    hashIndexSearchOnly = hashIndexSearchOnly
                  )
                else
                  mightContain(key) flatMap {
                    mightContain =>
                      if (mightContain)
                        get(
                          key = key,
                          start = floorValue,
                          end = getHigherForBinarySearch(),
                          hasRange = footer.hasRange,
                          hashIndexSearchOnly = hashIndexSearchOnly
                        )
                      else
                        IO.none
                  }
            }
        }
    }

  private def lower(key: Slice[Byte],
                    start: Option[Persistent],
                    end: Option[Persistent]): IO[Option[Persistent.SegmentResponse]] =
    blockCache.createBinarySearchIndexReader() flatMap {
      binarySearchIndexReader =>
        blockCache.createSortedIndexReader() flatMap {
          sortedIndexReader =>
            blockCache.createValuesReader() flatMap {
              valuesReader =>
                val endAt =
                  if (end.isDefined)
                    IO.Success(end)
                  else
                    get(key = key, hashIndexSearchOnly = true)

                endAt flatMap {
                  end =>
                    SegmentSearcher.searchLower(
                      key = key,
                      start = start,
                      end = end,
                      binarySearchIndexReader = binarySearchIndexReader,
                      sortedIndexReader = sortedIndexReader,
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
          Option(persistentCache.lowerEntry(key)).map(_.getValue) match {
            case someLower @ Some(lowerKeyValue) =>
              //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
              if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
                lowerKeyValue match {
                  case response: Persistent.SegmentResponse =>
                    IO.Success(Some(response))

                  case group: Persistent.Group =>
                    group.segment.lower(key)
                }
              else
                lowerKeyValue match {
                  case lowerRange: Persistent.Range if lowerRange containsLower key =>
                    IO.Success(Some(lowerRange))

                  case lowerGroup: Persistent.Group if lowerGroup containsLower key =>
                    lowerGroup.segment.lower(key)

                  case _ =>
                    Option(persistentCache.ceilingEntry(key)).map(_.getValue) match {
                      case someCeiling @ Some(ceilingKeyValue) =>
                        if (lowerKeyValue.nextIndexOffset == ceilingKeyValue.indexOffset)
                          lowerKeyValue match {
                            case response: Persistent.SegmentResponse =>
                              IO.Success(Some(response))

                            case group: Persistent.Group =>
                              group.segment.lower(key)
                          }
                        else
                          lower(key, someLower, someCeiling)

                      case None =>
                        lower(key, someLower, None)
                    }
                }

            case None =>
              val someCeiling = Option(persistentCache.ceilingEntry(key)).map(_.getValue)
              lower(key, None, someCeiling)
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

  private def higher(key: Slice[Byte],
                     start: Option[Persistent],
                     end: Option[Persistent]): IO[Option[Persistent.SegmentResponse]] =
    blockCache.createBinarySearchIndexReader() flatMap {
      binarySearchIndexReader =>
        blockCache.createSortedIndexReader() flatMap {
          sortedIndexReader =>
            blockCache.createValuesReader() flatMap {
              valuesReader =>
                val startFrom =
                  if (start.isDefined)
                    IO.Success(start)
                  else
                    get(key, hashIndexSearchOnly = true)

                startFrom flatMap {
                  startFrom =>
                    SegmentSearcher.searchHigher(
                      key = key,
                      start = startFrom,
                      end = end,
                      binarySearchIndexReader = binarySearchIndexReader,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader
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

  def higher(key: Slice[Byte]): IO[Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        IO.none

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        IO.none

      case _ =>
        Option(persistentCache.floorEntry(key)).map(_.getValue) match {
          case someFloor @ Some(floorEntry) =>
            floorEntry match {
              case floorRange: Persistent.Range if floorRange contains key =>
                IO.Success(Some(floorRange))

              case floorGroup: Persistent.Group if floorGroup containsHigher key =>
                floorGroup.segment.higher(key)

              case _ =>
                Option(persistentCache.higherEntry(key)).map(_.getValue) match {
                  case someHigher @ Some(higherKeyValue) =>
                    if (floorEntry.nextIndexOffset == higherKeyValue.indexOffset)
                      higherKeyValue match {
                        case response: Persistent.SegmentResponse =>
                          IO.Success(Some(response))

                        case group: Persistent.Group =>
                          group.segment.higher(key)
                      }
                    else
                      higher(key, someFloor, someHigher)

                  case None =>
                    higher(key, someFloor, None)
                }
            }

          case None =>
            val someHigher = Option(persistentCache.higherEntry(key)).map(_.getValue)
            higher(key, None, someHigher)
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[Slice[KeyValue.ReadOnly]] =
    blockCache.getFooter() flatMap {
      footer =>
        blockCache.createSortedIndexReader() flatMap {
          sortedIndexReader =>
            blockCache.createValuesReader() flatMap {
              valuesReader =>
                SortedIndexBlock
                  .readAll(
                    keyValueCount = footer.keyValueCount,
                    sortedIndexReader = sortedIndexReader,
                    valuesReader = valuesReader,
                    addTo = addTo
                  )
                  .onFailureSideEffect {
                    _ =>
                      logger.trace("{}: Reading sorted index block failed.", id)
                  }
            }
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

  def readAllBytes(): IO[Slice[Byte]] =
    blockCache.readAllBytes()

  def isInitialised() =
    blockCache.isCached
}

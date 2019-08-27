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

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Persistent, _}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.util.Options._
import swaydb.core.util.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[core] object SegmentCache {

  def apply(id: String,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            unsliceKey: Boolean,
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentIO)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  memorySweeper: Option[MemorySweeper.KeyValue]): SegmentCache =
    new SegmentCache(
      id = id,
      maxKey = maxKey,
      minKey = minKey,
      _skipList = when(memorySweeper.isDefined)(Some(SkipList.concurrent())),
      unsliceKey = unsliceKey,
      blockCache =
        SegmentBlockCache(
          id = id,
          blockRef = blockRef,
          segmentIO = segmentIO
        )
    )(keyOrder = keyOrder, memorySweeper = memorySweeper, groupIO = segmentIO)
}
private[core] class SegmentCache(id: String,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 _skipList: Option[SkipList[Slice[Byte], Persistent]],
                                 unsliceKey: Boolean,
                                 val blockCache: SegmentBlockCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                    memorySweeper: Option[MemorySweeper.KeyValue],
                                                                    groupIO: SegmentIO) extends LazyLogging {


  import keyOrder._

  private val threadStates = SegmentThreadState.create[Slice[Byte], Persistent]()

  def thisThreadState = threadStates.get()

  def skipList: SkipList[Slice[Byte], Persistent] =
    _skipList getOrElse thisThreadState.skipList

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
    if (!skipList.isConcurrent)
      skipList.put(keyValue.key, keyValue)
    else if (skipList.putIfAbsent(keyValue.key, keyValue))
      memorySweeper.foreach(_.add(keyValue, skipList))
  }

  private def addToCache(group: Persistent.Group): Unit = {
    if (unsliceKey) group.unsliceKeys
    if (!skipList.isConcurrent)
      skipList.put(group.key, group)
    else if (skipList.putIfAbsent(group.key, group))
      memorySweeper.foreach(_.add(group, skipList))
  }

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    skipList.get(key)

  def mightContain(key: Slice[Byte]): IO[swaydb.Error.Segment, Boolean] =
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
                  end: => Option[Persistent],
                  hasRange: Boolean,
                  keyValueCount: => IO[swaydb.Error.Segment, Int],
                  threadState: SegmentReadThreadState): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
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
                      keyValueCount = keyValueCount,
                      hashIndexReader = hashIndexReader,
                      binarySearchIndexReader = binarySearchIndexReader,
                      sortedIndexReader = sortedIndexReader,
                      valuesReader = valuesReader,
                      hasRange = hasRange,
                      threadState = Some(threadState)
                    ) flatMap {
                      case Some(response: Persistent.SegmentResponse) =>
                        addToCache(response)
                        IO.Right(Some(response))

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

  def get(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        IO.none

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        IO.none

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        IO.none

      case _ =>
        val threadState = thisThreadState
        val skipList = threadState.skipList

        skipList.floor(key) match {
          case Some(floor: Persistent.SegmentResponse) if floor.key equiv key =>
            IO.Right(Some(floor))

          //check if the key belongs to this group.
          case Some(group: Persistent.Group) if group contains key =>
            group.segment.get(key)

          case Some(floorRange: Persistent.Range) if floorRange contains key =>
            IO.Right(Some(floorRange))

          case floorValue =>
            if (key equiv minKey)
              blockCache.createSortedIndexReader() flatMap {
                sortedIndexReader =>
                  blockCache.createValuesReader() flatMap {
                    valuesReader =>
                      SortedIndexBlock.search(
                        key = key,
                        startFrom = None,
                        sortedIndexReader = sortedIndexReader,
                        valuesReader = valuesReader
                      ) flatMap {
                        case Some(response: Persistent.SegmentResponse) =>
                          addToCache(response)
                          IO.Right(Some(response))

                        case Some(group: Persistent.Group) =>
                          addToCache(group)
                          group.segment.get(key)

                        case None =>
                          IO.none
                      }
                  }
              }
            else
              blockCache.getFooter() flatMap {
                footer =>
                  if (footer.hasRange)
                    get(
                      key = key,
                      start = floorValue,
                      keyValueCount = IO.Right(footer.keyValueCount),
                      end = skipList.higher(key),
                      threadState = thisThreadState,
                      hasRange = footer.hasRange
                    )
                  else
                    mightContain(key) flatMap {
                      mightContain =>
                        if (mightContain)
                          get(
                            key = key,
                            start = floorValue,
                            keyValueCount = IO.Right(footer.keyValueCount),
                            threadState = thisThreadState,
                            end = skipList.higher(key),
                            hasRange = footer.hasRange
                          )
                        else
                          IO.none
                    }
              }
        }
    }

  private def lower(key: Slice[Byte],
                    start: Option[Persistent],
                    end: => Option[Persistent],
                    keyValueCount: => IO[swaydb.Error.Segment, Int]): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
    blockCache.createBinarySearchIndexReader() flatMap {
      binarySearchIndexReader =>
        blockCache.createSortedIndexReader() flatMap {
          sortedIndexReader =>
            blockCache.createValuesReader flatMap {
              valuesReader =>
                SegmentSearcher.searchLower(
                  key = key,
                  start = start,
                  end = end,
                  keyValueCount = keyValueCount,
                  binarySearchIndexReader = binarySearchIndexReader,
                  sortedIndexReader = sortedIndexReader,
                  valuesReader
                ) flatMap {
                  case Some(response: Persistent.SegmentResponse) =>
                    addToCache(response)
                    IO.Right(Some(response))

                  case Some(group: Persistent.Group) =>
                    addToCache(group)
                    group.segment.lower(key)

                  case None =>
                    IO.none
                }
            }
        }
    }

  private def getForLower(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Persistent]] =
    skipList.get(key) match {
      case some @ Some(_) =>
        IO(some)

      case None =>
        blockCache.getFooter() flatMap {
          footer =>
            if (footer.hasGroup) //don't do get if it has Group because it will fetch the inner group key-value which cannot be used as startFrom.
              IO.none
            else
              get(key = key)
        }
    }

  def lower(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
    if (key <= minKey)
      IO.none
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          get(maxKey)

        case MaxKey.Range(fromKey, _) if key > fromKey =>
          get(fromKey)

        case _ =>
          skipList.lower(key) match {
            case someLower @ Some(lowerKeyValue) =>
              //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
              if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
                lowerKeyValue match {
                  case response: Persistent.SegmentResponse =>
                    IO.Right(Some(response))

                  case group: Persistent.Group =>
                    group.segment.lower(key)
                }
              else
                lowerKeyValue match {
                  case lowerRange: Persistent.Range if lowerRange containsLower key =>
                    IO.Right(Some(lowerRange))

                  case lowerGroup: Persistent.Group if lowerGroup containsLower key =>
                    lowerGroup.segment.lower(key)

                  case lowerKeyValue: Persistent =>
                    getForLower(key) flatMap {
                      case Some(got) if lowerKeyValue.nextIndexOffset == got.indexOffset =>
                        lowerKeyValue match {
                          case response: Persistent.SegmentResponse =>
                            IO.Right(Some(response))

                          case group: Persistent.Group =>
                            group.segment.lower(key)
                        }

                      case someCeiling @ Some(ceilingRange: Persistent.Range) =>
                        if (ceilingRange containsLower key)
                          IO.Right(Some(ceilingRange))
                        else
                          lower(
                            key = key,
                            start = someLower,
                            end = someCeiling,
                            keyValueCount = getFooter().map(_.keyValueCount)
                          )

                      case someCeiling @ Some(ceilingGroup: Persistent.Group) =>
                        if (ceilingGroup containsLower key)
                          ceilingGroup.segment.lower(key)
                        else
                          lower(
                            key = key,
                            start = someLower,
                            end = someCeiling,
                            keyValueCount = getFooter().map(_.keyValueCount)
                          )

                      case someCeiling @ Some(_: Persistent.Fixed) =>
                        lower(
                          key = key,
                          start = someLower,
                          end = someCeiling,
                          keyValueCount = getFooter().map(_.keyValueCount)
                        )

                      case None =>
                        lower(
                          key = key,
                          start = someLower,
                          end = None,
                          keyValueCount = getFooter().map(_.keyValueCount)
                        )
                    }
                }

            case None =>
              lower(
                key = key,
                start = None,
                end = getForLower(key).toOption.flatten,
                keyValueCount = getFooter().map(_.keyValueCount)
              )
          }
      }

  def floorHigherHint(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Slice[Byte]]] =
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
                     end: => Option[Persistent],
                     keyValueCount: => IO[swaydb.Error.Segment, Int]): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
    blockCache.getFooter() flatMap {
      footer =>
        blockCache.createBinarySearchIndexReader() flatMap {
          binarySearchIndexReader =>
            blockCache.createSortedIndexReader() flatMap {
              sortedIndexReader =>
                blockCache.createValuesReader() flatMap {
                  valuesReader =>
                    val startFrom =
                      if (start.isDefined || footer.hasGroup) //don't do get if it has Group because it will fetch the inner group key-value which cannot be used as startFrom.
                        IO.Right(start)
                      else
                        get(key)

                    startFrom flatMap {
                      startFrom =>
                        SegmentSearcher.searchHigher(
                          key = key,
                          start = startFrom,
                          end = end,
                          keyValueCount = keyValueCount,
                          binarySearchIndexReader = binarySearchIndexReader,
                          sortedIndexReader = sortedIndexReader,
                          valuesReader = valuesReader
                        ) flatMap {
                          case Some(response: Persistent.SegmentResponse) =>
                            addToCache(response)
                            IO.Right(Some(response))

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
    }

  def higher(key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Persistent.SegmentResponse]] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        IO.none

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        IO.none

      case _ =>
        skipList.floor(key) match {
          case someFloor @ Some(floorEntry) =>
            floorEntry match {
              case floor: Persistent.Range if floor contains key =>
                IO.Right(Some(floor))

              case floor: Persistent.Group if floor containsHigher key =>
                floor.segment.higher(key)

              case _ =>
                skipList.higher(key) match {
                  case Some(higherRange: Persistent.Range) if higherRange contains key =>
                    IO.Right(Some(higherRange))

                  case Some(higherGroup: Persistent.Group) if higherGroup containsHigher key =>
                    higherGroup.segment.higher(key)

                  case someHigher @ Some(higherKeyValue) =>
                    if (floorEntry.nextIndexOffset == higherKeyValue.indexOffset)
                      higherKeyValue match {
                        case response: Persistent.SegmentResponse =>
                          IO.Right(Some(response))

                        case group: Persistent.Group =>
                          group.segment.higher(key)
                      }
                    else
                      higher(
                        key = key,
                        start = someFloor,
                        end = someHigher,
                        keyValueCount = getFooter().map(_.keyValueCount)
                      )

                  case None =>
                    higher(
                      key = key,
                      start = someFloor,
                      end = None,
                      keyValueCount = getFooter().map(_.keyValueCount)
                    )
                }
            }

          case None =>
            higher(
              key = key,
              start = None,
              end = skipList.higher(key),
              keyValueCount = getFooter().map(_.keyValueCount)
            )
        }
    }

  def getAll(addTo: Option[Slice[KeyValue.ReadOnly]] = None): IO[swaydb.Error.Segment, Slice[KeyValue.ReadOnly]] =
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
                  .onLeftSideEffect {
                    _ =>
                      logger.trace("{}: Reading sorted index block failed.", id)
                  }
            }
        }
    }

  def getHeadKeyValueCount(): IO[swaydb.Error.Segment, Int] =
    blockCache.getFooter().map(_.keyValueCount)

  def getBloomFilterKeyValueCount(): IO[swaydb.Error.Segment, Int] =
    blockCache.getFooter().map(_.bloomFilterItemsCount)

  def getFooter(): IO[swaydb.Error.Segment, SegmentFooterBlock] =
    blockCache.getFooter()

  def hasRange: IO[swaydb.Error.Segment, Boolean] =
    blockCache.getFooter().map(_.hasRange)

  def hasPut: IO[swaydb.Error.Segment, Boolean] =
    blockCache.getFooter().map(_.hasPut)

  def isKeyValueCacheEmpty =
    skipList.isEmpty

  def isBlockCacheEmpty =
    !blockCache.isCached

  def isFooterDefined: Boolean =
    blockCache.isFooterDefined

  def hasBloomFilter: IO[swaydb.Error.Segment, Boolean] =
    blockCache.getFooter().map(_.bloomFilterOffset.isDefined)

  def createdInLevel: IO[swaydb.Error.Segment, Int] =
    blockCache.getFooter().map(_.createdInLevel)

  def isGrouped: IO[swaydb.Error.Segment, Boolean] =
    blockCache.getFooter().map(_.hasGroup)

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList contains key

  def cacheSize: Int =
    skipList.size

  def clearCachedKeyValues() =
    skipList.clear()

  def clearLocalAndBlockCache() = {
    threadStates.clear()
    blockCache.clear()
  }

  def areAllCachesEmpty =
    isKeyValueCacheEmpty && !blockCache.isCached

  def readAllBytes(): IO[swaydb.Error.Segment, Slice[Byte]] =
    blockCache.readAllBytes()

  def isInitialised() =
    blockCache.isCached
}

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

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Persistent, _}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.reader.BlockRefReader
import swaydb.core.util.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable

private[core] object SegmentCache {

  def apply(path: Path,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            unsliceKey: Boolean,
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentIO)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentCache =
    new SegmentCache(
      path = path,
      maxKey = maxKey,
      minKey = minKey,
      skipList =
        keyValueMemorySweeper map {
          sweeper =>
            sweeper.maxKeyValuesPerSegment match {
              case Some(maxKeyValuesPerSegment) =>
                SkipList.concurrent(maxKeyValuesPerSegment)

              case None =>
                SkipList.concurrent()
            }
        },
      unsliceKey = unsliceKey,
      blockCache =
        SegmentBlockCache(
          path = path,
          blockRef = blockRef,
          segmentIO = segmentIO
        )
    )
}

private[core] class SegmentCache(path: Path,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 val skipList: Option[SkipList[Slice[Byte], Persistent]],
                                 unsliceKey: Boolean,
                                 val blockCache: SegmentBlockCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                    blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                                    keyValueMemorySweeper: Option[MemorySweeper.KeyValue]) extends LazyLogging {

  import keyOrder._

  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  /**
   * Notes for why use putIfAbsent before adding to cache:
   *
   * Sometimes file seeks will be done if the last known cached key-value's ranges are smaller than the
   * key being searched. For example: Search key is 10, but the last lower cache key-value range is 1-5.
   * here it's unknown if a lower key 7 exists without doing a file seek. This is also one of the reasons
   * reverse iterations are slower than forward.
   */
  private def addToCache(keyValue: Persistent): Unit = {
    if (unsliceKey) keyValue.unsliceKeys
    skipList foreach {
      skipList =>
        if (skipList.putIfAbsent(keyValue.key, keyValue))
          keyValueMemorySweeper.foreach(_.add(keyValue, skipList))
    }
  }

  def getFromCache(key: Slice[Byte]): Option[Persistent] =
    skipList.flatMap(_.get(key))

  def mightContain(key: Slice[Byte]): Boolean =
    blockCache
      .createBloomFilterReader()
      .forall {
        bloomFilterReader =>
          BloomFilterBlock.mightContain(
            key = key,
            reader = bloomFilterReader
          )
      }

  private def get(key: Slice[Byte],
                  start: Option[Persistent],
                  end: => Option[Persistent],
                  hasRange: Boolean,
                  keyValueCount: Int,
                  readState: ReadState): Option[Persistent] =
    SegmentSearcher.search(
      path = path,
      key = key,
      start = start,
      end = end,
      keyValueCount = keyValueCount,
      hashIndexReader = blockCache.createHashIndexReader(),
      binarySearchIndexReader = blockCache.createBinarySearchIndexReader(),
      sortedIndexReader = blockCache.createSortedIndexReader(),
      valuesReader = blockCache.createValuesReader(),
      hasRange = hasRange,
      readState = readState
    ) match {
      case resp @ Some(_) =>
        addToCache(resp.get)
        resp

      case None =>
        None
    }

  def get(key: Slice[Byte],
          readState: ReadState): Option[Persistent] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        None

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        None

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        None

      case _ =>
        skipList.flatMap(_.floor(key)) match {
          case got @ Some(floor: Persistent) if floor.key equiv key =>
            got

          case got @ Some(floorRange: Persistent.Range) if floorRange contains key =>
            got

          case floorValue =>
            val footer = blockCache.getFooter()
            if (footer.hasRange)
              get(
                key = key,
                start = floorValue,
                keyValueCount = footer.keyValueCount,
                end = skipList.flatMap(_.higher(key)),
                readState = readState,
                hasRange = footer.hasRange
              )
            else if (mightContain(key))
              get(
                key = key,
                start = floorValue,
                keyValueCount = footer.keyValueCount,
                readState = readState,
                end = skipList.flatMap(_.higher(key)),
                hasRange = footer.hasRange
              )
            else
              None
        }
    }

  private def lower(key: Slice[Byte],
                    start: Option[Persistent],
                    end: => Option[Persistent],
                    keyValueCount: => Int): Option[Persistent] =
    SegmentSearcher.searchLower(
      key = key,
      start = start,
      end = end,
      keyValueCount = keyValueCount,
      binarySearchIndexReader = blockCache.createBinarySearchIndexReader(),
      sortedIndexReader = blockCache.createSortedIndexReader(),
      valuesReader = blockCache.createValuesReader()
    ) match {
      case resp @ Some(_) =>
        addToCache(resp.get)
        resp

      case None =>
        None
    }

  private def getForLower(key: Slice[Byte],
                          readState: ReadState): Option[Persistent] =
    skipList.flatMap(_.get(key)) orElse get(key, readState)

  def lower(key: Slice[Byte],
            readState: ReadState): Option[Persistent] =
    if (key <= minKey)
      None
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          get(maxKey, readState)

        case MaxKey.Range(fromKey, _) if key > fromKey =>
          get(fromKey, readState)

        case _ =>
          skipList.flatMap(_.lower(key)) match {
            case someLower @ Some(lowerKeyValue) =>
              //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
              if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
                lowerKeyValue match {
                  case response: Persistent =>
                    Some(response)
                }
              else
                lowerKeyValue match {
                  case lowerRange: Persistent.Range if lowerRange containsLower key =>
                    someLower

                  case lowerKeyValue: Persistent =>
                    getForLower(key, readState) match {
                      case someCeiling @ Some(ceilingRange: Persistent.Range) =>
                        if (ceilingRange containsLower key)
                          Some(ceilingRange)
                        else if (lowerKeyValue.nextIndexOffset == ceilingRange.indexOffset)
                          someLower
                        else
                          lower(
                            key = key,
                            start = someLower,
                            end = someCeiling,
                            keyValueCount = getFooter().keyValueCount
                          )

                      case someCeiling @ Some(ceiling: Persistent.Fixed) =>
                        if (lowerKeyValue.nextIndexOffset == ceiling.indexOffset)
                          someLower
                        else
                          lower(
                            key = key,
                            start = someLower,
                            end = someCeiling,
                            keyValueCount = getFooter().keyValueCount
                          )

                      case None =>
                        lower(
                          key = key,
                          start = someLower,
                          end = None,
                          keyValueCount = getFooter().keyValueCount
                        )
                    }
                }

            case None =>
              lower(
                key = key,
                start = None,
                end = getForLower(key, readState),
                keyValueCount = getFooter().keyValueCount
              )
          }
      }

  def floorHigherHint(key: Slice[Byte]): Option[Slice[Byte]] =
    if (hasPut)
      if (key < minKey)
        Some(minKey)
      else if (key < maxKey.maxKey)
        Some(key)
      else
        None
    else
      None

  private def higher(key: Slice[Byte],
                     start: Option[Persistent],
                     end: => Option[Persistent],
                     keyValueCount: => Int): Option[Persistent] =
    SegmentSearcher.searchHigher(
      key = key,
      start = start,
      end = end,
      keyValueCount = keyValueCount,
      binarySearchIndexReader = blockCache.createBinarySearchIndexReader(),
      sortedIndexReader = blockCache.createSortedIndexReader(),
      valuesReader = blockCache.createValuesReader()
    ) match {
      case resp @ Some(_) =>
        addToCache(resp.get)
        resp

      case None =>
        None
    }

  def higher(key: Slice[Byte],
             readState: ReadState): Option[Persistent] =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        None

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        None

      case _ =>
        skipList.flatMap(_.floor(key)) match {
          case someFloor @ Some(floorEntry) =>
            floorEntry match {
              case floor: Persistent.Range if floor contains key =>
                someFloor

              case _ =>
                skipList.flatMap(_.higher(key)) match {
                  case someHigher @ Some(higherRange: Persistent.Range) if higherRange contains key =>
                    someHigher

                  case someHigher @ Some(higherKeyValue) =>
                    if (floorEntry.nextIndexOffset == higherKeyValue.indexOffset)
                      someHigher
                    else
                      higher(
                        key = key,
                        start = someFloor,
                        end = someHigher,
                        keyValueCount = getFooter().keyValueCount
                      )

                  case None =>
                    higher(
                      key = key,
                      start = someFloor,
                      end = None,
                      keyValueCount = getFooter().keyValueCount
                    )
                }
            }

          case None =>
            get(key, readState) match {
              case some @ Some(floor: Persistent.Range) if floor contains key =>
                some

              case start =>
                higher(
                  key = key,
                  start = start,
                  end = skipList.flatMap(_.higher(key)),
                  keyValueCount = getFooter().keyValueCount
                )
            }
        }
    }

  def getAll[T](builder: mutable.Builder[KeyValue, T]): Unit =
    blockCache readAll builder

  def getAll(): Slice[KeyValue] =
    blockCache.readAll()

  def getAll(keyValueCount: Int): Slice[KeyValue] =
    blockCache readAll keyValueCount

  def getKeyValueCount(): Int =
    blockCache.getFooter().keyValueCount

  def getFooter(): SegmentFooterBlock =
    blockCache.getFooter()

  def hasRange: Boolean =
    blockCache.getFooter().hasRange

  def hasPut: Boolean =
    blockCache.getFooter().hasPut

  def isKeyValueCacheEmpty =
    skipList.forall(_.isEmpty)

  def isBlockCacheEmpty =
    !blockCache.isCached

  def isFooterDefined: Boolean =
    blockCache.isFooterDefined

  def hasBloomFilter: Boolean =
    blockCache.getFooter().bloomFilterOffset.isDefined

  def createdInLevel: Int =
    blockCache.getFooter().createdInLevel

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList.exists(_.contains(key))

  def cacheSize: Int =
    skipList.foldLeft(0)(_ + _.size)

  def clearCachedKeyValues() =
    skipList.foreach(_.clear())

  def clearBlockCache() =
    blockCache.clear()

  def areAllCachesEmpty =
    isKeyValueCacheEmpty && !blockCache.isCached

  def readAllBytes(): Slice[Byte] =
    blockCache.readAllBytes()

  def isInitialised() =
    blockCache.isCached
}

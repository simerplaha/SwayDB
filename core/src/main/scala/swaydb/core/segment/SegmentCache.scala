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
import swaydb.Aggregator
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Persistent, _}
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.util.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.util.SomeOrNone._

private[core] object SegmentCache {

  def apply(path: Path,
            maxKey: MaxKey[Slice[Byte]],
            minKey: Slice[Byte],
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentIO,
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
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
                SkipList.concurrent(
                  limit = maxKeyValuesPerSegment,
                  nullKey = Slice.Null,
                  nullValue = Persistent.Null
                )

              case None =>
                SkipList.concurrent(
                  nullKey = Slice.Null,
                  nullValue = Persistent.Null
                )
            }
        },
      blockCache =
        SegmentBlockCache(
          path = path,
          segmentIO = segmentIO,
          blockRef = blockRef,
          valuesReaderCacheable = valuesReaderCacheable,
          sortedIndexReaderCacheable = sortedIndexReaderCacheable,
          hashIndexReaderCacheable = hashIndexReaderCacheable,
          binarySearchIndexReaderCacheable = binarySearchIndexReaderCacheable,
          bloomFilterReaderCacheable = bloomFilterReaderCacheable,
          footerCacheable = footerCacheable
        )
    )
}

private[core] class SegmentCache(path: Path,
                                 maxKey: MaxKey[Slice[Byte]],
                                 minKey: Slice[Byte],
                                 val skipList: Option[SkipList[SliceOptional[Byte], PersistentOptional, Slice[Byte], Persistent]],
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
    skipList foreach {
      skipList =>
        //unslice not required anymore since SegmentSearch always unsliced.
        //keyValue.unsliceKeys
        if (skipList.putIfAbsent(keyValue.key, keyValue))
          keyValueMemorySweeper.foreach(_.add(keyValue, skipList))
    }
  }

  def getFromCache(key: Slice[Byte]): PersistentOptional =
    skipList match {
      case Some(skipList) =>
        skipList get key

      case None =>
        Persistent.Null
    }

  def mightContain(key: Slice[Byte]): Boolean = {
    val bloomFilterReader = blockCache.createBloomFilterReaderNullable()
    bloomFilterReader == null ||
      BloomFilterBlock.mightContain(
        key = key,
        reader = bloomFilterReader
      )
  }

  def get(key: Slice[Byte],
          readState: ReadState): PersistentOptional =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key > maxKey =>
        Persistent.Null

      case range: MaxKey.Range[Slice[Byte]] if key >= range.maxKey =>
        Persistent.Null

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        None

      case _ =>
        skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.floor(key)) match {
          case floor: Persistent if floor.key equiv key =>
            floor

          case floorRange: Persistent.Range if floorRange contains key =>
            floorRange

          case floorValue =>
            val footer = blockCache.getFooter()
            if (footer.hasRange || mightContain(key))
              SegmentSearcher.search(
                path = path,
                key = key,
                start = floorValue,
                end = skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.higher(key)),
                keyValueCount = footer.keyValueCount,
                hashIndexReaderNullable = blockCache.createHashIndexReaderNullable(),
                binarySearchIndexReaderNullable = blockCache.createBinarySearchIndexReaderNullable(),
                sortedIndexReader = blockCache.createSortedIndexReader(),
                valuesReaderNullable = blockCache.createValuesReaderNullable(),
                hasRange = footer.hasRange,
                readState = readState
              ).sizeEffect(addToCache)
            else
              Persistent.Null
        }
    }

  def higher(key: Slice[Byte],
             readState: ReadState): PersistentOptional =
    maxKey match {
      case MaxKey.Fixed(maxKey) if key >= maxKey =>
        Persistent.Null

      case MaxKey.Range(_, maxKey) if key >= maxKey =>
        Persistent.Null

      case _ =>
        skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.floor(key)) match {
          case someFloor: Persistent =>
            someFloor match {
              case floor: Persistent.Range if floor contains key =>
                someFloor

              case _ =>
                skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.higher(key)) match {
                  case higher: Persistent.Range if higher contains key =>
                    higher

                  case higher: Persistent =>
                    if (someFloor.nextIndexOffset == higher.indexOffset)
                      higher
                    else
                      SegmentSearcher.searchHigher(
                        key = key,
                        start = someFloor,
                        end = higher,
                        keyValueCount = getFooter().keyValueCount,
                        readState = readState,
                        binarySearchIndexReaderNullable = blockCache.createBinarySearchIndexReaderNullable(),
                        sortedIndexReader = blockCache.createSortedIndexReader(),
                        valuesReaderNullable = blockCache.createValuesReaderNullable()
                      ).sizeEffect(addToCache)

                  case Persistent.Null =>
                    SegmentSearcher.searchHigher(
                      key = key,
                      start = someFloor,
                      end = Persistent.Null,
                      keyValueCount = getFooter().keyValueCount,
                      readState = readState,
                      binarySearchIndexReaderNullable = blockCache.createBinarySearchIndexReaderNullable(),
                      sortedIndexReader = blockCache.createSortedIndexReader(),
                      valuesReaderNullable = blockCache.createValuesReaderNullable()
                    ).sizeEffect(addToCache)
                }
            }

          case Persistent.Null =>
            get(key, readState) match {
              case floor: Persistent.Range if floor contains key =>
                floor

              case start =>
                SegmentSearcher.searchHigher(
                  key = key,
                  start = start,
                  end = skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.higher(key)),
                  keyValueCount = getFooter().keyValueCount,
                  readState = readState,
                  binarySearchIndexReaderNullable = blockCache.createBinarySearchIndexReaderNullable(),
                  sortedIndexReader = blockCache.createSortedIndexReader(),
                  valuesReaderNullable = blockCache.createValuesReaderNullable()
                ).sizeEffect(addToCache)
            }
        }
    }

  private def lower(key: Slice[Byte],
                    start: PersistentOptional,
                    end: => PersistentOptional,
                    keyValueCount: => Int): PersistentOptional =
    SegmentSearcher.searchLower(
      key = key,
      start = start,
      end = end,
      keyValueCount = keyValueCount,
      binarySearchIndexReaderNullable = blockCache.createBinarySearchIndexReaderNullable(),
      sortedIndexReader = blockCache.createSortedIndexReader(),
      valuesReaderNullable = blockCache.createValuesReaderNullable()
    ).sizeEffect(addToCache)

  private def getForLower(key: Slice[Byte],
                          readState: ReadState): PersistentOptional =
    skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.get(key)) orElseS get(key, readState)

  def lower(key: Slice[Byte],
            readState: ReadState): PersistentOptional =
    if (key <= minKey)
      Persistent.Null
    else
      maxKey match {
        case MaxKey.Fixed(maxKey) if key > maxKey =>
          get(maxKey, readState)

        case MaxKey.Range(fromKey, _) if key > fromKey =>
          get(fromKey, readState)

        case _ =>
          skipList.flatMapOption(Persistent.Null: PersistentOptional)(_.lower(key)) match {
            case lowerKeyValue: Persistent =>
              //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
              if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
                lowerKeyValue
              else
                lowerKeyValue match {
                  case lowerRange: Persistent.Range if lowerRange containsLower key =>
                    lowerRange

                  case lowerKeyValue: Persistent =>
                    getForLower(key, readState) match {
                      case ceilingRange: Persistent.Range =>
                        if (ceilingRange containsLower key)
                          ceilingRange
                        else if (lowerKeyValue.nextIndexOffset == ceilingRange.indexOffset)
                          lowerKeyValue
                        else
                          lower(
                            key = key,
                            start = lowerKeyValue,
                            end = ceilingRange,
                            keyValueCount = getFooter().keyValueCount
                          )

                      case ceiling: Persistent.Fixed =>
                        if (lowerKeyValue.nextIndexOffset == ceiling.indexOffset)
                          lowerKeyValue
                        else
                          lower(
                            key = key,
                            start = lowerKeyValue,
                            end = ceiling,
                            keyValueCount = getFooter().keyValueCount
                          )

                      case Persistent.Null =>
                        lower(
                          key = key,
                          start = lowerKeyValue,
                          end = Persistent.Null,
                          keyValueCount = getFooter().keyValueCount
                        )
                    }
                }

            case Persistent.Null =>
              lower(
                key = key,
                start = Persistent.Null,
                end = getForLower(key, readState),
                keyValueCount = getFooter().keyValueCount
              )
          }
      }

  def getAll[T](aggregator: Aggregator[KeyValue, T]): Unit =
    blockCache readAll aggregator

  def getAll(): Slice[KeyValue] =
    blockCache.readAll()

  def getAll(keyValueCount: Int): Slice[KeyValue] =
    blockCache readAll keyValueCount

  def iterator(): Iterator[Persistent] =
    blockCache.iterator()

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

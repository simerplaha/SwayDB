/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.{Persistent, _}
import swaydb.core.function.FunctionStore
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.segment.data.TransientSegment
import swaydb.core.segment.format.a.block.segment.footer.SegmentFooterBlock
import swaydb.core.segment.format.a.block.segment.{SegmentBlock, SegmentBlockCache}
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.MinMax
import swaydb.core.util.skiplist.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.slice.Slice._

import swaydb.data.util.{SomeOrNone, TupleOrNone}

import scala.collection.mutable.ListBuffer
import scala.collection.compat._

private[core] sealed trait SegmentRefOption extends SomeOrNone[SegmentRefOption, SegmentRef] {
  override def noneS: SegmentRefOption = SegmentRef.Null
}

private[core] object SegmentRef {

  final case object Null extends SegmentRefOption {
    override def isNoneS: Boolean = true

    override def getS: SegmentRef = throw new Exception("SegmentRef is of type Null")
  }

  def apply(path: Path,
            minKey: Sliced[Byte],
            maxKey: MaxKey[Sliced[Byte]],
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentIO,
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                                         blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef = {
    val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Sliced[Byte], Persistent]] =
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
      }

    val segmentBlockCache =
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

    new SegmentRef(
      path = path,
      maxKey = maxKey,
      minKey = minKey,
      skipList = skipList,
      segmentBlockCache = segmentBlockCache
    )
  }

  def bestStartForGetOrHigherSearch(key: Sliced[Byte],
                                    segmentState: SegmentReadStateOption,
                                    floorFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                                                         persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =
    if (segmentState.isSomeS)
      SegmentRef.bestStartForGetOrHigherSearch(
        key = key,
        keyValueFromState = segmentState.getS.keyValue._2,
        floorFromSkipList = floorFromSkipList
      )
    else
      floorFromSkipList

  /**
   * Finds the best key to start from fetch highest.
   *
   * @param key               The key being searched
   * @param keyValueFromState KeyValue read from the [[ThreadReadState]]. This keyValue could also
   *                          be higher than key itself so a check to ensure that key is <=
   *                          than the search key is required.
   * @param floorFromSkipList KeyValue read from the [[SegmentRef.skipList]]. This will always
   *                          be <= to the key being search.
   * @return the best possible key-value to search higher search from.
   */
  def bestStartForGetOrHigherSearch(key: Sliced[Byte],
                                    keyValueFromState: Persistent,
                                    floorFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                                                         persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =
    if (floorFromSkipList.isNoneS)
      if (keyOrder.lteq(keyValueFromState.getS.key, key))
        keyValueFromState
      else
        Persistent.Null
    else
      MinMax.minFavourLeft[Persistent](
        left = keyValueFromState.getS,
        right = floorFromSkipList.getS
      )

  def bestEndForLowerSearch(key: Sliced[Byte],
                            segmentState: SegmentReadStateOption,
                            ceilingFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                                                   persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =
    if (segmentState.isSomeS && segmentState.getS.lower.isSomeC)
      SegmentRef.bestEndForLowerSearch(
        key = key,
        lowerKeyValueFromState = segmentState.getS.lower.getC.right,
        ceilingFromSkipList = ceilingFromSkipList
      )
    else
      ceilingFromSkipList

  def bestEndForLowerSearch(key: Sliced[Byte],
                            lowerKeyValueFromState: Persistent,
                            ceilingFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                                                   persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =
    if (ceilingFromSkipList.isNoneS)
      if (keyOrder.gteq(lowerKeyValueFromState.getS.key, key))
        lowerKeyValueFromState
      else
        Persistent.Null
    else
      MinMax.minFavourLeft[Persistent](
        left = lowerKeyValueFromState.getS,
        right = ceilingFromSkipList.getS
      )

  def get(key: Sliced[Byte],
          threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                        keyOrder: KeyOrder[Sliced[Byte]],
                                        partialKeyOrder: KeyOrder[Persistent.Partial],
                                        persistentKeyOrder: KeyOrder[Persistent],
                                        segmentSearcher: SegmentSearcher): PersistentOption = {
    //    println(s"Get: ${key.readInt()}")
    segmentRef.maxKey match {
      case MaxKey.Fixed(maxKey) if keyOrder.gt(key, maxKey) =>
        Persistent.Null

      case range: MaxKey.Range[Sliced[Byte]] if keyOrder.gteq(key, range.maxKey) =>
        Persistent.Null

      //check for minKey inside the Segment is not required since Levels already do minKey check.
      //      case _ if key < minKey =>
      //        None

      case _ =>
        val footer = segmentRef.segmentBlockCache.getFooter()
        val segmentStateOptional = threadState.getSegmentState(segmentRef.path)
        val getFromState =
          if (segmentStateOptional.isSomeS)
            segmentStateOptional.getS.keyValue._2 match {
              case fixed: Persistent if keyOrder.equiv(fixed.key, key) =>
                fixed

              case range: Persistent.Range if KeyValue.Range.contains(range, key) =>
                range

              case _ =>
                Persistent.Null
            }
          else
            Persistent.Null

        if (getFromState.isSomeS)
          getFromState.getS
        else
          segmentRef.applyToSkipList(_.floor(key)) match {
            case floor: Persistent if keyOrder.equiv(floor.key, key) =>
              floor

            case floorRange: Persistent.Range if KeyValue.Range.contains(floorRange, key) =>
              floorRange

            case floorValue =>
              if (footer.hasRange || segmentRef.mightContain(key)) {
                val bestStart =
                  SegmentRef.bestStartForGetOrHigherSearch(
                    key = key,
                    segmentState = segmentStateOptional,
                    floorFromSkipList = floorValue
                  )

                /**
                 * First try searching sequentially.
                 */
                val sequentialRead =
                  if (segmentStateOptional.isNoneS || segmentStateOptional.getS.isSequential)
                    segmentSearcher.searchSequential(
                      key = key,
                      start = bestStart,
                      sortedIndexReader = segmentRef.segmentBlockCache.createSortedIndexReader(),
                      valuesReaderOrNull = segmentRef.segmentBlockCache.createValuesReaderOrNull()
                    ) onSomeSideEffectS {
                      found =>
                        SegmentReadState.updateOnSuccessSequentialRead(
                          path = segmentRef.path,
                          forKey = key,
                          segmentState = segmentStateOptional,
                          threadReadState = threadState,
                          found = found
                        )
                        segmentRef addToSkipList found
                    }
                  else
                    Persistent.Null

                if (sequentialRead.isSomeS) {
                  sequentialRead
                } else {
                  val higher = segmentRef.applyToSkipList(_.higher(key))
                  if (bestStart.existsS(bestStart => higher.existsS(_.indexOffset == bestStart.nextIndexOffset)))
                    Persistent.Null
                  else
                    segmentSearcher.searchRandom(
                      key = key,
                      start = bestStart,
                      end = higher,
                      keyValueCount = footer.keyValueCount,
                      hashIndexReaderOrNull = segmentRef.segmentBlockCache.createHashIndexReaderOrNull(),
                      binarySearchIndexReaderOrNull = segmentRef.segmentBlockCache.createBinarySearchIndexReaderOrNull(),
                      sortedIndexReader = segmentRef.segmentBlockCache.createSortedIndexReader(),
                      valuesReaderOrNull = segmentRef.segmentBlockCache.createValuesReaderOrNull(),
                      hasRange = footer.hasRange
                    ) onSideEffectS {
                      found =>
                        SegmentReadState.updateAfterRandomRead(
                          path = segmentRef.path,
                          forKey = key,
                          start = bestStart,
                          segmentStateOptional = segmentStateOptional,
                          threadReadState = threadState,
                          foundOption = found
                        )

                        found foreachS segmentRef.addToSkipList
                    }
                }
              }
              else
                Persistent.Null
          }
    }
  }

  def higher(key: Sliced[Byte],
             threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                           keyOrder: KeyOrder[Sliced[Byte]],
                                           persistentKeyOrder: KeyOrder[Persistent],
                                           partialKeyOrder: KeyOrder[Persistent.Partial],
                                           segmentSearcher: SegmentSearcher): PersistentOption =
    segmentRef.maxKey match {
      case MaxKey.Fixed(maxKey) if keyOrder.gteq(key, maxKey) =>
        Persistent.Null

      case MaxKey.Range(_, maxKey) if keyOrder.gteq(key, maxKey) =>
        Persistent.Null

      case _ =>
        if (keyOrder.lt(key, segmentRef.minKey)) {
          get(segmentRef.minKey, threadState)
        } else {
          val segmentStateOptional = threadState.getSegmentState(segmentRef.path)

          //check for cases where the segment has gaped key-values.
          //key = 5, keys = {1, 2, 3, 4, 5, 10, 11, 12}
          //here higher for 5 will return 10, if 6 is provided then no IO is required and 10 is returned immediately.
          if (segmentStateOptional.isSomeS && keyOrder.lteq(segmentStateOptional.getS.keyValue._1, key) && keyOrder.gt(segmentStateOptional.getS.keyValue._2.key, key)) {
            segmentStateOptional.getS.keyValue._2
          } else {
            val blockCache = segmentRef.segmentBlockCache
            val footer = blockCache.getFooter()

            //check if range matches the higher condition without IO.
            val higherFromState =
              if (footer.hasRange && segmentStateOptional.isSomeS)
                segmentStateOptional.getS.keyValue._2 match {
                  case range: Persistent.Range if KeyValue.Range.contains(range, key) =>
                    range

                  case _ =>
                    Persistent.Null
                }
              else
                Persistent.Null

            if (higherFromState.isSomeS)
              higherFromState.getS
            else
              segmentRef.applyToSkipList(_.floor(key)) match {
                case floor: Persistent.Range if KeyValue.Range.contains(floor, key) =>
                  floor

                case floor =>
                  segmentRef.applyToSkipList(_.higher(key)) match {
                    case higher: Persistent.Range if KeyValue.Range.contains(higher, key) =>
                      higher

                    case higher =>
                      val inMemorySeek =
                      //check if in-memory skipList satisfies higher without IO.
                        if (floor.isSomeS && higher.isSomeS && floor.getS.nextIndexOffset == higher.getS.indexOffset) {
                          SegmentReadState.updateOnSuccessSequentialRead(
                            path = segmentRef.path,
                            forKey = key,
                            segmentState = segmentStateOptional,
                            threadReadState = threadState,
                            found = higher.getS
                          )
                          higher
                        } else {
                          Persistent.Null
                        }

                      if (inMemorySeek.isSomeS) {
                        inMemorySeek
                      } else {
                        //IO required
                        val bestStart =
                          SegmentRef.bestStartForGetOrHigherSearch(
                            key = key,
                            segmentState = segmentStateOptional,
                            floorFromSkipList = floor
                          ) // having orElseS get(key, readState) here is resulting is slower performance.

                        val sequentialSeek =
                          if (segmentStateOptional.isNoneS || segmentStateOptional.getS.isSequential)
                            segmentSearcher.searchHigherSequentially(
                              key = key,
                              start = bestStart,
                              sortedIndexReader = blockCache.createSortedIndexReader(),
                              valuesReaderOrNull = blockCache.createValuesReaderOrNull()
                            ) onSomeSideEffectS {
                              found =>
                                SegmentReadState.updateOnSuccessSequentialRead(
                                  path = segmentRef.path,
                                  forKey = key,
                                  segmentState = segmentStateOptional,
                                  threadReadState = threadState,
                                  found = found
                                )
                                segmentRef addToSkipList found
                            }
                          else
                            Persistent.Null

                        if (sequentialSeek.isSomeS)
                          sequentialSeek
                        else
                          segmentSearcher.searchHigherRandomly(
                            key = key,
                            start = bestStart,
                            end = higher,
                            keyValueCount = segmentRef.getFooter().keyValueCount,
                            binarySearchIndexReaderOrNull = blockCache.createBinarySearchIndexReaderOrNull(),
                            sortedIndexReader = blockCache.createSortedIndexReader(),
                            valuesReaderOrNull = blockCache.createValuesReaderOrNull()
                          ) onSideEffectS {
                            optional =>
                              SegmentReadState.updateAfterRandomRead(
                                path = segmentRef.path,
                                forKey = key,
                                start = bestStart,
                                segmentStateOptional = segmentStateOptional,
                                threadReadState = threadState,
                                foundOption = optional
                              )

                              optional foreachS segmentRef.addToSkipList
                          }
                      }
                  }
              }
          }
        }
    }

  private def lower(key: Sliced[Byte],
                    start: PersistentOption,
                    end: PersistentOption,
                    keyValueCount: Int,
                    path: Path,
                    segmentStateOptional: SegmentReadStateOption,
                    threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                                  keyOrder: KeyOrder[Sliced[Byte]],
                                                  persistentKeyOrder: KeyOrder[Persistent],
                                                  partialKeyOrder: KeyOrder[Persistent.Partial],
                                                  segmentSearcher: SegmentSearcher): PersistentOption = {

    val sortedIndexReader = segmentRef.segmentBlockCache.createSortedIndexReader()
    val endKeyValue =
      if (end.isNoneS && sortedIndexReader.block.enableAccessPositionIndex)
      //end is only helpful for lower if accessPositionIndex is enabled.
      //this get is only invoked if lower seeks are performed incrementally (1 to 100) instead of 100 to 1
      //Stream by default run in reverse so 100 to 1 so this will not be invoked unless done manually.
        get(key, threadState)
      else
        end

    segmentSearcher.searchLower(
      key = key,
      start = start,
      end = endKeyValue,
      keyValueCount = keyValueCount,
      binarySearchIndexReaderOrNull = segmentRef.segmentBlockCache.createBinarySearchIndexReaderOrNull(),
      sortedIndexReader = sortedIndexReader,
      valuesReaderOrNull = segmentRef.segmentBlockCache.createValuesReaderOrNull()
    ) onSideEffectS {
      optional =>
        optional foreachS {
          found =>
            found.unsliceKeys
            val unslicedKey = key.unslice()

            segmentStateOptional match {
              case SegmentReadState.Null =>
                threadState.setSegmentState(
                  path = path,
                  nextIndexOffset =
                    new SegmentReadState(
                      keyValue = (unslicedKey, found),
                      lower = TupleOrNone.Some(unslicedKey, found),
                      isSequential = true
                    )
                )

              case state: SegmentReadState =>
                state.lower = TupleOrNone.Some(unslicedKey, found)
            }
            segmentRef addToSkipList found
        }
    }
  }

  private def bestEndForLowerSearch(key: Sliced[Byte],
                                    segmentState: SegmentReadStateOption,
                                    readState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                                                keyOrder: KeyOrder[Sliced[Byte]],
                                                                persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =

    SegmentRef.bestEndForLowerSearch(
      key = key,
      segmentState = segmentState,
      ceilingFromSkipList = segmentRef.applyToSkipList(_.ceiling(key))
    )

  def lower(key: Sliced[Byte],
            threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                          keyOrder: KeyOrder[Sliced[Byte]],
                                          persistentKeyOrder: KeyOrder[Persistent],
                                          partialKeyOrder: KeyOrder[Persistent.Partial],
                                          segmentSearcher: SegmentSearcher): PersistentOption =
    if (keyOrder.lteq(key, segmentRef.minKey))
      Persistent.Null
    else
      segmentRef.maxKey match {
        case MaxKey.Fixed(maxKey) if keyOrder.gt(key, maxKey) =>
          get(maxKey, threadState)

        case MaxKey.Range(fromKey, _) if keyOrder.gt(key, fromKey) =>
          get(fromKey, threadState)

        case _ =>
          val segmentStateOption = threadState.getSegmentState(segmentRef.path)
          //check for cases where the segment has gaped key-values.
          //key = 5, keys = {1, 2, 3, 4, 5, 10, 11, 12}
          //here lower for 9 will return 5, if 8 is provided then no IO is required and 5 is returned immediately.
          if (segmentStateOption.isSomeS && segmentStateOption.getS.lower.isSomeC && keyOrder.gteq(segmentStateOption.getS.lower.getC.left, key) && keyOrder.lt(segmentStateOption.getS.lower.getC.right.key, key)) {
            segmentStateOption.getS.lower.getC.right
          } else {
            val blockCache = segmentRef.segmentBlockCache
            val footer = blockCache.getFooter()
            val lowerFromState =
              if (segmentStateOption.isSomeS)
                //using foundKeyValue here instead of foundLowerKeyValue because foundKeyValue is always == foundLowerKeyValue if previous seek was lower
                //if not then foundKeyValue gives a higher chance of being lower for cases with random reads were performed.
                segmentStateOption.getS.keyValue._2 match {
                  case range: Persistent.Range if KeyValue.Range.containsLower(range, key) =>
                    range

                  case _ =>
                    Persistent.Null
                }
              else
                Persistent.Null

            if (lowerFromState.isSomeS)
              lowerFromState.getS
            else
              segmentRef.applyToSkipList(_.lower(key)) match {
                case lowerKeyValue: Persistent =>
                  //if the lowest key-value in the cache is the last key-value, then lower is the next lowest key-value for the key.
                  if (lowerKeyValue.nextIndexOffset == -1) //-1 indicated last key-value in the Segment.
                    lowerKeyValue
                  else
                    lowerKeyValue match {
                      case lowerRange: Persistent.Range if KeyValue.Range.containsLower(lowerRange, key) =>
                        lowerRange

                      case lowerKeyValue: Persistent =>
                        bestEndForLowerSearch(
                          key = key,
                          segmentState = segmentStateOption,
                          readState = threadState
                        ) match {
                          case ceilingRange: Persistent.Range =>
                            if (KeyValue.Range.containsLower(ceilingRange, key))
                              ceilingRange
                            else if (lowerKeyValue.nextIndexOffset == ceilingRange.indexOffset)
                              lowerKeyValue
                            else
                              lower(
                                key = key,
                                start = lowerKeyValue,
                                end = ceilingRange,
                                keyValueCount = footer.keyValueCount,
                                path = segmentRef.path,
                                segmentStateOptional = segmentStateOption,
                                threadState = threadState
                              )

                          case ceiling: Persistent.Fixed =>
                            if (lowerKeyValue.nextIndexOffset == ceiling.indexOffset)
                              lowerKeyValue
                            else
                              lower(
                                key = key,
                                start = lowerKeyValue,
                                end = ceiling,
                                keyValueCount = footer.keyValueCount,
                                path = segmentRef.path,
                                segmentStateOptional = segmentStateOption,
                                threadState = threadState
                              )

                          case Persistent.Null =>
                            lower(
                              key = key,
                              start = lowerKeyValue,
                              end = Persistent.Null,
                              keyValueCount = footer.keyValueCount,
                              path = segmentRef.path,
                              segmentStateOptional = segmentStateOption,
                              threadState = threadState
                            )
                        }
                    }

                case Persistent.Null =>
                  lower(
                    key = key,
                    start = Persistent.Null,
                    end = bestEndForLowerSearch(key, segmentStateOption, threadState),
                    keyValueCount = footer.keyValueCount,
                    path = segmentRef.path,
                    segmentStateOptional = segmentStateOption,
                    threadState = threadState
                  )
              }
          }
      }

  def put(ref: SegmentRef,
          newKeyValues: Sliced[KeyValue],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                              timeOrder: TimeOrder[Sliced[Byte]],
                                              functionStore: FunctionStore): Sliced[TransientSegment] =
    put(
      oldKeyValuesCount = ref.getKeyValueCount(),
      oldKeyValues = ref.iterator(),
      newKeyValues = newKeyValues,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    )

  def put(oldKeyValuesCount: Int,
          oldKeyValues: Iterator[Persistent],
          newKeyValues: Sliced[KeyValue],
          removeDeletes: Boolean,
          createdInLevel: Int,
          valuesConfig: ValuesBlock.Config,
          sortedIndexConfig: SortedIndexBlock.Config,
          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
          hashIndexConfig: HashIndexBlock.Config,
          bloomFilterConfig: BloomFilterBlock.Config,
          segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Sliced[Byte]],
                                              timeOrder: TimeOrder[Sliced[Byte]],
                                              functionStore: FunctionStore): Sliced[TransientSegment] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](ListBuffer.newBuilder)

    SegmentMerger.merge(
      newKeyValues = newKeyValues,
      oldKeyValuesCount = oldKeyValuesCount,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed = builder.close(sortedIndexConfig.enableAccessPositionIndex)

    SegmentBlock.writeOneOrMany(
      mergeStats = closed,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def refresh(ref: SegmentRef,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Sliced[Byte]]): Sliced[TransientSegment] = {

    val footer = ref.getFooter()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.
    if (footer.createdInLevel == createdInLevel) {
      val iterator = ref.iterator()
      refreshForSameLevel(
        sortedIndexBlock = ref.segmentBlockCache.getSortedIndex(),
        valuesBlock = ref.segmentBlockCache.getValues(),
        iterator = iterator,
        keyValuesCount = footer.keyValueCount,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )
    }
    else
      refreshForNewLevel(
        keyValues = ref.iterator(),
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )
  }

  /**
   * This refresh does not compute new sortedIndex size and uses existing. Saves an iteration
   * and performs refresh instantly.
   */
  def refreshForSameLevel(sortedIndexBlock: SortedIndexBlock,
                          valuesBlock: Option[ValuesBlock],
                          iterator: Iterator[Persistent],
                          keyValuesCount: Int,
                          removeDeletes: Boolean,
                          createdInLevel: Int,
                          valuesConfig: ValuesBlock.Config,
                          sortedIndexConfig: SortedIndexBlock.Config,
                          binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                          hashIndexConfig: HashIndexBlock.Config,
                          bloomFilterConfig: BloomFilterBlock.Config,
                          segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Sliced[Byte]]): Sliced[TransientSegment] = {

    val sortedIndexSize =
      sortedIndexBlock.compressionInfo match {
        case Some(compressionInfo) =>
          compressionInfo.decompressedLength

        case None =>
          sortedIndexBlock.offset.size
      }

    val valuesSize =
      valuesBlock match {
        case Some(valuesBlock) =>
          valuesBlock.compressionInfo match {
            case Some(value) =>
              value.decompressedLength

            case None =>
              valuesBlock.offset.size
          }
        case None =>
          0
      }

    val keyValues =
      Segment
        .toMemoryIterator(iterator, removeDeletes)
        .to(Iterable)

    val mergeStats =
      new MergeStats.Persistent.Closed[Iterable](
        isEmpty = false,
        keyValuesCount = keyValuesCount,
        keyValues = keyValues,
        totalValuesSize = valuesSize,
        maxSortedIndexSize = sortedIndexSize
      )

    SegmentBlock.writeOneOrMany(
      mergeStats = mergeStats,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }

  def refreshForNewLevel(keyValues: Iterator[Persistent],
                         removeDeletes: Boolean,
                         createdInLevel: Int,
                         valuesConfig: ValuesBlock.Config,
                         sortedIndexConfig: SortedIndexBlock.Config,
                         binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                         hashIndexConfig: HashIndexBlock.Config,
                         bloomFilterConfig: BloomFilterBlock.Config,
                         segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Sliced[Byte]]): Sliced[TransientSegment] = {
    val memoryKeyValues =
      Segment
        .toMemoryIterator(keyValues, removeDeletes)
        .to(Iterable)

    val builder =
      MergeStats
        .persistentBuilder(memoryKeyValues)
        .close(sortedIndexConfig.enableAccessPositionIndex)

    SegmentBlock.writeOneOrMany(
      mergeStats = builder,
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    )
  }
}

private[core] class SegmentRef(val path: Path,
                               val maxKey: MaxKey[Sliced[Byte]],
                               val minKey: Sliced[Byte],
                               val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Sliced[Byte], Persistent]],
                               val segmentBlockCache: SegmentBlockCache)(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                         keyOrder: KeyOrder[Sliced[Byte]]) extends SegmentRefOption with LazyLogging {

  override def isNoneS: Boolean =
    false

  override def getS: SegmentRef =
    this

  /**
   * Notes for why use putIfAbsent before adding to cache:
   *
   * Sometimes file seeks will be done if the last known cached key-value's ranges are smaller than the
   * key being searched. For example: Search key is 10, but the last lower cache key-value range is 1-5.
   * here it's unknown if a lower key 7 exists without doing a file seek. This is also one of the reasons
   * reverse iterations are slower than forward.
   */
  private def addToSkipList(keyValue: Persistent): Unit =
    skipList foreach {
      skipList =>
        //unslice not required anymore since SegmentSearch always unsliced.
        //keyValue.unsliceKeys
        if (skipList.putIfAbsent(keyValue.key, keyValue))
          keyValueMemorySweeper.foreach(_.add(keyValue, skipList))
    }

  private def applyToSkipList(f: SkipList[SliceOption[Byte], PersistentOption, Sliced[Byte], Persistent] => PersistentOption): PersistentOption =
    if (skipList.isDefined)
      f(skipList.get)
    else
      Persistent.Null

  def getFromCache(key: Sliced[Byte]): PersistentOption =
    skipList match {
      case Some(skipList) =>
        skipList get key

      case None =>
        Persistent.Null
    }

  def mightContain(key: Sliced[Byte]): Boolean = {
    val bloomFilterReader = segmentBlockCache.createBloomFilterReaderOrNull()
    bloomFilterReader == null ||
      BloomFilterBlock.mightContain(
        comparableKey = keyOrder.comparableKey(key),
        reader = bloomFilterReader
      )
  }

  def toSlice(): Sliced[Persistent] =
    segmentBlockCache.toSlice()

  def toSlice(keyValueCount: Int): Sliced[Persistent] =
    segmentBlockCache toSlice keyValueCount

  def iterator(): Iterator[Persistent] =
    segmentBlockCache.iterator()

  def getKeyValueCount(): Int =
    segmentBlockCache.getFooter().keyValueCount

  def getFooter(): SegmentFooterBlock =
    segmentBlockCache.getFooter()

  def hasRange: Boolean =
    segmentBlockCache.getFooter().hasRange

  def hasPut: Boolean =
    segmentBlockCache.getFooter().hasPut

  def isKeyValueCacheEmpty =
    skipList.forall(_.isEmpty)

  def isBlockCacheEmpty =
    !segmentBlockCache.isCached

  def isFooterDefined: Boolean =
    segmentBlockCache.isFooterDefined

  def hasBloomFilter: Boolean =
    segmentBlockCache.getFooter().bloomFilterOffset.isDefined

  def createdInLevel: Int =
    segmentBlockCache.getFooter().createdInLevel

  def isInKeyValueCache(key: Sliced[Byte]): Boolean =
    skipList.exists(_.contains(key))

  def cacheSize: Int =
    skipList.foldLeft(0)(_ + _.size)

  def clearCachedKeyValues() =
    skipList.foreach(_.clear())

  def clearBlockCache() =
    segmentBlockCache.clear()

  def areAllCachesEmpty =
    isKeyValueCacheEmpty && !segmentBlockCache.isCached

  def readAllBytes(): Sliced[Byte] =
    segmentBlockCache.readAllBytes()

  def segmentSize: Int =
    segmentBlockCache.segmentSize

  def isInitialised() =
    segmentBlockCache.isCached

}

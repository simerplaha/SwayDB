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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment

import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.{Persistent, _}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.assigner.{Assignable, SegmentAssigner}
import swaydb.core.segment.format.a.block.BlockCache
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
import swaydb.core.util.ParallelCollection._
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListConcurrentLimit}
import swaydb.core.util.{IDGenerator, MinMax}
import swaydb.data.MaxKey
import swaydb.data.compaction.ParallelMerge.SegmentParallelism
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}
import swaydb.data.util.{SomeOrNoneCovariant, TupleOrNone}
import swaydb.{Aggregator, IO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

private[core] sealed trait SegmentRefOption extends SomeOrNoneCovariant[SegmentRefOption, SegmentRef] {
  override def noneC: SegmentRefOption = SegmentRef.Null
}

private[core] case object SegmentRef extends LazyLogging {

  final case object Null extends SegmentRefOption {
    override def isNoneC: Boolean = true

    override def getC: SegmentRef = throw new Exception("SegmentRef is of type Null")
  }

  def apply(path: Path,
            minKey: Slice[Byte],
            maxKey: MaxKey[Slice[Byte]],
            nearestPutDeadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            blockRef: BlockRefReader[SegmentBlock.Offset],
            segmentIO: SegmentIO,
            valuesReaderCacheable: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            sortedIndexReaderCacheable: Option[UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock]],
            hashIndexReaderCacheable: Option[UnblockedReader[HashIndexBlock.Offset, HashIndexBlock]],
            binarySearchIndexReaderCacheable: Option[UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock]],
            bloomFilterReaderCacheable: Option[UnblockedReader[BloomFilterBlock.Offset, BloomFilterBlock]],
            footerCacheable: Option[SegmentFooterBlock])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         blockCacheMemorySweeper: Option[MemorySweeper.Block],
                                                         keyValueMemorySweeper: Option[MemorySweeper.KeyValue]): SegmentRef = {
    val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent]] =
      keyValueMemorySweeper map {
        sweeper =>
          sweeper.maxKeyValuesPerSegment match {
            case Some(maxKeyValuesPerSegment) =>
              SkipListConcurrentLimit(
                limit = maxKeyValuesPerSegment,
                nullKey = Slice.Null,
                nullValue = Persistent.Null
              )

            case None =>
              SkipListConcurrent(
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
      nearestPutDeadline = nearestPutDeadline,
      minMaxFunctionId = minMaxFunctionId,
      skipList = skipList,
      segmentBlockCache = segmentBlockCache
    )
  }

  def bestStartForGetOrHigherSearch(key: Slice[Byte],
                                    segmentState: SegmentReadStateOption,
                                    floorFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Slice[Byte]],
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
  def bestStartForGetOrHigherSearch(key: Slice[Byte],
                                    keyValueFromState: Persistent,
                                    floorFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Slice[Byte]],
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

  def bestEndForLowerSearch(key: Slice[Byte],
                            segmentState: SegmentReadStateOption,
                            ceilingFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                   persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =
    if (segmentState.isSomeS && segmentState.getS.lower.isSomeC)
      SegmentRef.bestEndForLowerSearch(
        key = key,
        lowerKeyValueFromState = segmentState.getS.lower.getC.right,
        ceilingFromSkipList = ceilingFromSkipList
      )
    else
      ceilingFromSkipList

  def bestEndForLowerSearch(key: Slice[Byte],
                            lowerKeyValueFromState: Persistent,
                            ceilingFromSkipList: PersistentOption)(implicit keyOrder: KeyOrder[Slice[Byte]],
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

  def get(key: Slice[Byte],
          threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                        keyOrder: KeyOrder[Slice[Byte]],
                                        partialKeyOrder: KeyOrder[Persistent.Partial],
                                        persistentKeyOrder: KeyOrder[Persistent],
                                        segmentSearcher: SegmentSearcher): PersistentOption = {
    //    println(s"Get: ${key.readInt()} - ${segmentRef.path}")
    segmentRef.maxKey match {
      case MaxKey.Fixed(maxKey) if keyOrder.gt(key, maxKey) =>
        Persistent.Null

      case range: MaxKey.Range[Slice[Byte]] if keyOrder.gteq(key, range.maxKey) =>
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
              if (footer.hasRange || segmentRef.mightContainKey(key, threadState)) {
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

  def higher(key: Slice[Byte],
             threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                           keyOrder: KeyOrder[Slice[Byte]],
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

  private def lower(key: Slice[Byte],
                    start: PersistentOption,
                    end: PersistentOption,
                    keyValueCount: Int,
                    path: Path,
                    segmentStateOptional: SegmentReadStateOption,
                    threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                                  keyOrder: KeyOrder[Slice[Byte]],
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

  private def bestEndForLowerSearch(key: Slice[Byte],
                                    segmentState: SegmentReadStateOption,
                                    readState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                                                keyOrder: KeyOrder[Slice[Byte]],
                                                                persistentKeyOrder: KeyOrder[Persistent]): PersistentOption =

    SegmentRef.bestEndForLowerSearch(
      key = key,
      segmentState = segmentState,
      ceilingFromSkipList = segmentRef.applyToSkipList(_.ceiling(key))
    )

  def lower(key: Slice[Byte],
            threadState: ThreadReadState)(implicit segmentRef: SegmentRef,
                                          keyOrder: KeyOrder[Slice[Byte]],
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

  def mergePut(ref: SegmentRef,
               headGap: Iterable[Assignable],
               tailGap: Iterable[Assignable],
               mergeableCount: Int,
               mergeable: Iterator[Assignable],
               removeDeletes: Boolean,
               createdInLevel: Int,
               valuesConfig: ValuesBlock.Config,
               sortedIndexConfig: SortedIndexBlock.Config,
               binarySearchIndexConfig: BinarySearchIndexBlock.Config,
               hashIndexConfig: HashIndexBlock.Config,
               bloomFilterConfig: BloomFilterBlock.Config,
               segmentConfig: SegmentBlock.Config,
               pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                   keyOrder: KeyOrder[Slice[Byte]],
                                                   timeOrder: TimeOrder[Slice[Byte]],
                                                   functionStore: FunctionStore,
                                                   blockCacheSweeper: Option[MemorySweeper.Block],
                                                   fileSweeper: FileSweeper,
                                                   bufferCleaner: ByteBufferSweeperActor,
                                                   keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                   forceSaveApplier: ForceSaveApplier,
                                                   segmentIO: SegmentIO): SegmentPutResult[Slice[PersistentSegment]] = {
    //if it's the last Level do full merge to clear any removed key-values.
    val segments =
      SegmentRef.mergeWrite(
        ref = ref,
        headGap = headGap,
        tailGap = tailGap,
        mergeableCount = mergeableCount,
        mergeable = mergeable,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    val newSegments =
      Segment.persistent(
        pathsDistributor = pathsDistributor,
        createdInLevel = createdInLevel,
        mmap = segmentConfig.mmap,
        transient = segments
      )

    new SegmentPutResult[Slice[PersistentSegment]](result = newSegments, replaced = true)
  }

  @inline def mergeWrite(ref: SegmentRef,
                         headGap: Iterable[Assignable],
                         tailGap: Iterable[Assignable],
                         mergeableCount: Int,
                         mergeable: Iterator[Assignable],
                         removeDeletes: Boolean,
                         createdInLevel: Int,
                         valuesConfig: ValuesBlock.Config,
                         sortedIndexConfig: SortedIndexBlock.Config,
                         binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                         hashIndexConfig: HashIndexBlock.Config,
                         bloomFilterConfig: BloomFilterBlock.Config,
                         segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                             timeOrder: TimeOrder[Slice[Byte]],
                                                             functionStore: FunctionStore): Slice[TransientSegment] =
    mergeWrite(
      oldKeyValuesCount = ref.getKeyValueCount(),
      oldKeyValues = ref.iterator(),
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    )

  def mergeWrite(oldKeyValuesCount: Int,
                 oldKeyValues: Iterator[Persistent],
                 headGap: Iterable[Assignable],
                 tailGap: Iterable[Assignable],
                 mergeableCount: Int,
                 mergeable: Iterator[Assignable],
                 removeDeletes: Boolean,
                 createdInLevel: Int,
                 valuesConfig: ValuesBlock.Config,
                 sortedIndexConfig: SortedIndexBlock.Config,
                 binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                 hashIndexConfig: HashIndexBlock.Config,
                 bloomFilterConfig: BloomFilterBlock.Config,
                 segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore): Slice[TransientSegment] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    SegmentMerger.merge(
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      oldKeyValuesCount = oldKeyValuesCount,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed =
      builder.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

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

  @inline def mergeWriteOne(ref: SegmentRef,
                            headGap: Iterable[Assignable],
                            tailGap: Iterable[Assignable],
                            mergeableCount: Int,
                            mergeable: Iterator[Assignable],
                            removeDeletes: Boolean,
                            createdInLevel: Int,
                            valuesConfig: ValuesBlock.Config,
                            sortedIndexConfig: SortedIndexBlock.Config,
                            binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                            hashIndexConfig: HashIndexBlock.Config,
                            bloomFilterConfig: BloomFilterBlock.Config,
                            segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                timeOrder: TimeOrder[Slice[Byte]],
                                                                functionStore: FunctionStore): Slice[TransientSegment.One] =
    mergeWriteOne(
      oldKeyValuesCount = ref.getKeyValueCount(),
      oldKeyValues = ref.iterator(),
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      removeDeletes = removeDeletes,
      createdInLevel = createdInLevel,
      valuesConfig = valuesConfig,
      sortedIndexConfig = sortedIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      hashIndexConfig = hashIndexConfig,
      bloomFilterConfig = bloomFilterConfig,
      segmentConfig = segmentConfig
    )

  def mergeWriteOne(oldKeyValuesCount: Int,
                    oldKeyValues: Iterator[Persistent],
                    headGap: Iterable[Assignable],
                    tailGap: Iterable[Assignable],
                    mergeableCount: Int,
                    mergeable: Iterator[Assignable],
                    removeDeletes: Boolean,
                    createdInLevel: Int,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore): Slice[TransientSegment.One] = {

    val builder = MergeStats.persistent[Memory, ListBuffer](Aggregator.listBuffer)

    SegmentMerger.merge(
      headGap = headGap,
      tailGap = tailGap,
      mergeableCount = mergeableCount,
      mergeable = mergeable,
      oldKeyValuesCount = oldKeyValuesCount,
      oldKeyValues = oldKeyValues,
      stats = builder,
      isLastLevel = removeDeletes
    )

    val closed =
      builder.close(
        hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
        optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
      )

    SegmentBlock.writeOnes(
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

  def fastPutMany(assignables: Iterable[Iterable[Assignable]],
                  segmentParallelism: SegmentParallelism,
                  removeDeletes: Boolean,
                  createdInLevel: Int,
                  valuesConfig: ValuesBlock.Config,
                  sortedIndexConfig: SortedIndexBlock.Config,
                  binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                  hashIndexConfig: HashIndexBlock.Config,
                  bloomFilterConfig: BloomFilterBlock.Config,
                  segmentConfig: SegmentBlock.Config,
                  pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                      executionContext: ExecutionContext,
                                                      keyOrder: KeyOrder[Slice[Byte]],
                                                      timeOrder: TimeOrder[Slice[Byte]],
                                                      functionStore: FunctionStore,
                                                      blockCacheSweeper: Option[MemorySweeper.Block],
                                                      fileSweeper: FileSweeper,
                                                      bufferCleaner: ByteBufferSweeperActor,
                                                      keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                      forceSaveApplier: ForceSaveApplier,
                                                      segmentIO: SegmentIO): IO[swaydb.Error.Segment, Iterable[Slice[PersistentSegment]]] =
    assignables.mapParallel[Slice[PersistentSegment]](parallelism = segmentParallelism.parallelism, timeout = segmentParallelism.timeout)(
      block =
        gap =>
          IO {
            SegmentRef.fastPut(
              assignable = gap,
              removeDeletes = removeDeletes,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig,
              pathsDistributor = pathsDistributor
            )
          },
      recover = {
        case (segments, _) =>
          segments.foreach(_.foreach(_.delete))
      }
    )

  def fastPut(assignable: Iterable[Assignable],
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                  functionStore: FunctionStore,
                                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                                  fileSweeper: FileSweeper,
                                                  bufferCleaner: ByteBufferSweeperActor,
                                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                  forceSaveApplier: ForceSaveApplier,
                                                  segmentIO: SegmentIO): Slice[PersistentSegment] = {

    val stats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())

    val newSegments = ListBuffer.empty[PersistentSegment]

    try {
      assignable foreach {
        case collection: Assignable.Collection =>
          collection match {
            case segment: Segment =>
              Segment.copyToPersist(
                segment = segment,
                createdInLevel = createdInLevel,
                pathsDistributor = pathsDistributor,
                removeDeletes = removeDeletes,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig
              ) foreach {
                segment =>
                  newSegments += segment
              }

            case _ =>
              collection.iterator() foreach stats.add
          }

        case value: KeyValue =>
          stats add value
      }

      val mergeStats =
        stats.close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        )

      val gapedSegments =
        Segment.persistent(
          pathsDistributor = pathsDistributor,
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          mergeStats = mergeStats
        )

      (gapedSegments ++ newSegments).sortBy(_.key)(keyOrder)
    } catch {
      case throwable: Throwable =>
        newSegments.foreach(_.delete)

        throw throwable
    }
  }

  /**
   * GOAL: The goal of this is to avoid create small segments and to parallelise only if
   * there is enough work for two threads otherwise do the work in the current thread.
   *
   * On failure perform clean up.
   */
  def fastPut(ref: SegmentRef,
              headGap: Iterable[Assignable],
              tailGap: Iterable[Assignable],
              mergeableCount: Int,
              mergeable: Iterator[Assignable],
              removeDeletes: Boolean,
              createdInLevel: Int,
              segmentParallelism: SegmentParallelism,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config,
              pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                  executionContext: ExecutionContext,
                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                  functionStore: FunctionStore,
                                                  blockCacheSweeper: Option[MemorySweeper.Block],
                                                  fileSweeper: FileSweeper,
                                                  bufferCleaner: ByteBufferSweeperActor,
                                                  keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                  forceSaveApplier: ForceSaveApplier,
                                                  segmentIO: SegmentIO): SegmentPutResult[Slice[PersistentSegment]] = {

    //collect all new Segments for cleanup in-case there is a failure.
    val recoveryQueue = new ConcurrentLinkedQueue[PersistentSegment]()

    def putGap(gap: Iterable[Assignable]): Slice[PersistentSegment] = {
      val gapSegments =
        fastPut(
          assignable = gap,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig,
          pathsDistributor = pathsDistributor
        )

      gapSegments foreach recoveryQueue.add

      gapSegments
    }

    //if mergeable is empty min gap requirement is 0 so that Segments get copied else
    //there should be at least of quarter or 100 key-values for gaps to be created.
    val minGapSize = if (mergeableCount == 0) 0 else (mergeableCount / 4) max 100

    val (mergeableHead, mergeableTail, future) =
      Segment.runOnGapsParallel[Slice[PersistentSegment]](
        headGap = headGap,
        tailGap = tailGap,
        empty = PersistentSegment.emptySlice,
        minGapSize = minGapSize,
        segmentParallelism = segmentParallelism
      )(thunk = putGap)

    def putMid(): Slice[PersistentSegment] = {
      val segments =
        SegmentRef.mergeWrite(
          ref = ref,
          headGap = mergeableHead,
          tailGap = mergeableTail,
          mergeableCount = mergeableCount,
          mergeable = mergeable,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

      val midSegment =
        Segment.persistent(
          pathsDistributor = pathsDistributor,
          createdInLevel = createdInLevel,
          mmap = segmentConfig.mmap,
          transient = segments
        )

      midSegment foreach recoveryQueue.add

      midSegment
    }

    if (mergeableCount > 0) {
      try {
        val midSegments = putMid()
        val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)

        val newSegments = Slice.of[PersistentSegment](headSegments.size + midSegments.size + tailSegments.size)
        newSegments addAll headSegments
        newSegments addAll midSegments
        newSegments addAll tailSegments

        SegmentPutResult(result = newSegments, replaced = true)
      } catch {
        case throwable: Throwable =>
          //delete now.
          recoveryQueue.forEach(_.delete)

          //also do delete after the future is complete
          future.onComplete {
            _ =>
              recoveryQueue.forEach(_.delete)
          }

          throw throwable
      }
    } else {
      val (headSegments, tailSegments) =
        try
          Await.result(future, segmentParallelism.timeout)
        catch {
          case throwable: Throwable =>
            recoveryQueue.forEach(_.delete)
            throw throwable
        }

      val newSegments = headSegments ++ tailSegments

      SegmentPutResult(result = newSegments, replaced = false)
    }
  }

  def fastWriteOne(assignables: Iterable[Assignable],
                   createdInLevel: Int,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                                       executionContext: ExecutionContext,
                                                       keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                       fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       forceSaveApplier: ForceSaveApplier,
                                                       segmentIO: SegmentIO): ListBuffer[TransientSegment.One] = {

    val stats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())

    val finalOnes = ListBuffer.empty[TransientSegment.One]

    assignables foreach {
      case collection: Assignable.Collection =>
        val collectionStats = MergeStats.persistent[KeyValue, ListBuffer](Aggregator.listBuffer)(_.toMemory())
        collection.iterator() foreach collectionStats.add

        SegmentBlock.writeOnes(
          mergeStats =
            collectionStats.close(
              hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
              optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
            ),
          createdInLevel = createdInLevel,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig
        ) foreach {
          segment =>
            finalOnes += segment
        }

      case value: KeyValue =>
        stats add value
    }

    SegmentBlock.writeOnes(
      mergeStats =
        stats.close(
          hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
          optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
        ),
      createdInLevel = createdInLevel,
      bloomFilterConfig = bloomFilterConfig,
      hashIndexConfig = hashIndexConfig,
      binarySearchIndexConfig = binarySearchIndexConfig,
      sortedIndexConfig = sortedIndexConfig,
      valuesConfig = valuesConfig,
      segmentConfig = segmentConfig
    ) foreach {
      segment =>
        finalOnes += segment
    }

    finalOnes
  }

  def fastWriteOne(ref: SegmentRef,
                   headGap: Iterable[Assignable],
                   tailGap: Iterable[Assignable],
                   mergeableCount: Int,
                   mergeable: Iterator[Assignable],
                   removeDeletes: Boolean,
                   createdInLevel: Int,
                   segmentParallelism: SegmentParallelism,
                   valuesConfig: ValuesBlock.Config,
                   sortedIndexConfig: SortedIndexBlock.Config,
                   binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                   hashIndexConfig: HashIndexBlock.Config,
                   bloomFilterConfig: BloomFilterBlock.Config,
                   segmentConfig: SegmentBlock.Config)(implicit idGenerator: IDGenerator,
                                                       executionContext: ExecutionContext,
                                                       keyOrder: KeyOrder[Slice[Byte]],
                                                       timeOrder: TimeOrder[Slice[Byte]],
                                                       functionStore: FunctionStore,
                                                       blockCacheSweeper: Option[MemorySweeper.Block],
                                                       fileSweeper: FileSweeper,
                                                       bufferCleaner: ByteBufferSweeperActor,
                                                       keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                       forceSaveApplier: ForceSaveApplier,
                                                       segmentIO: SegmentIO): SegmentPutResult[ListBuffer[TransientSegment.One]] = {

    def writeGap(gap: Iterable[Assignable]): ListBuffer[TransientSegment.One] =
      fastWriteOne(
        assignables = gap,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    //if mergeable is empty min gap requirement is 0 so that Segments get copied else
    //there should be at least of quarter or 100 key-values for gaps to be created.
    val minGapSize = if (mergeableCount == 0) 0 else (mergeableCount / 4) max 100

    val (mergeableHead, mergeableTail, future) =
      Segment.runOnGapsParallel[ListBuffer[TransientSegment.One]](
        headGap = headGap,
        tailGap = tailGap,
        empty = ListBuffer.empty[TransientSegment.One],
        minGapSize = minGapSize,
        segmentParallelism = segmentParallelism
      )(thunk = writeGap)

    if (mergeableCount > 0) {
      val midSegments =
        SegmentRef.mergeWriteOne(
          ref = ref,
          headGap = mergeableHead,
          tailGap = mergeableTail,
          mergeableCount = mergeableCount,
          mergeable = mergeable,
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig
        )

      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)
      val newSegments = headSegments ++ midSegments ++ tailSegments
      SegmentPutResult(result = newSegments, replaced = true)
    } else {
      val (headSegments, tailSegments) = Await.result(future, segmentParallelism.timeout)
      val newSegments = headSegments ++ tailSegments
      SegmentPutResult(result = newSegments, replaced = false)
    }
  }

  def fastAssignPut(headGap: Iterable[Assignable],
                    tailGap: Iterable[Assignable],
                    segmentRefs: Iterator[SegmentRef],
                    assignableCount: Int,
                    assignables: Iterator[Assignable],
                    removeDeletes: Boolean,
                    createdInLevel: Int,
                    segmentParallelism: SegmentParallelism,
                    valuesConfig: ValuesBlock.Config,
                    sortedIndexConfig: SortedIndexBlock.Config,
                    binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                    hashIndexConfig: HashIndexBlock.Config,
                    bloomFilterConfig: BloomFilterBlock.Config,
                    segmentConfig: SegmentBlock.Config,
                    pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                        executionContext: ExecutionContext,
                                                        keyOrder: KeyOrder[Slice[Byte]],
                                                        timeOrder: TimeOrder[Slice[Byte]],
                                                        functionStore: FunctionStore,
                                                        blockCacheSweeper: Option[MemorySweeper.Block],
                                                        fileSweeper: FileSweeper,
                                                        bufferCleaner: ByteBufferSweeperActor,
                                                        keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                        segmentIO: SegmentIO,
                                                        forceSaveApplier: ForceSaveApplier): SegmentPutResult[Slice[PersistentSegment]] = {
    if (assignableCount == 0) {
      //if there are no assignments write gaps and return.
      val gapInsert =
        SegmentRef.fastPutMany(
          assignables = Seq(headGap, tailGap),
          removeDeletes = removeDeletes,
          createdInLevel = createdInLevel,
          segmentParallelism = segmentParallelism,
          valuesConfig = valuesConfig,
          sortedIndexConfig = sortedIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          hashIndexConfig = hashIndexConfig,
          bloomFilterConfig = bloomFilterConfig,
          segmentConfig = segmentConfig,
          pathsDistributor = pathsDistributor
        ).get

      if (gapInsert.isEmpty) {
        SegmentPutResult(result = Slice.empty, replaced = false)
      } else {
        val gapSlice = Slice.of[PersistentSegment](gapInsert.foldLeft(0)(_ + _.size))
        gapInsert foreach gapSlice.addAll
        SegmentPutResult(result = gapSlice, replaced = false)
      }
    } else {
      val (assignmentSegments, untouchedSegments) = segmentRefs.duplicate

      //assign key-values to Segment and then perform merge.
      val assignments =
        SegmentAssigner.assignUnsafeGapsSegmentRef[ListBuffer[Assignable]](
          assignablesCount = assignableCount,
          assignables = assignables,
          segments = assignmentSegments
        )

      if (assignments.isEmpty) {
        val exception = swaydb.Exception.MergeKeyValuesWithoutTargetSegment(assignableCount)
        val error = "Assigned segments are empty."
        logger.error(error, exception)
        throw exception
      } else {
        //keep oldRefs that are not assign and make sure they are added in order.

        def nextOldOrNull() = if (untouchedSegments.hasNext) untouchedSegments.next() else null

        val singles = ListBuffer.empty[TransientSegment.Singleton]

        if (headGap.nonEmpty)
          singles ++=
            fastWriteOne(
              assignables = headGap,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

        assignments foreach {
          assignment =>

            var oldRef: SegmentRef = nextOldOrNull()

            //insert SegmentRefs directly that are not assigned or do not require merging making
            //sure they are inserted in order.
            while (oldRef != null && oldRef != assignment.segment) {
              singles += TransientSegment.Remote(fileHeader = Slice.emptyBytes, ref = oldRef)

              oldRef = nextOldOrNull()
            }

            val result =
              SegmentRef.fastWriteOne(
                ref = assignment.segment,
                headGap = assignment.headGap.result,
                tailGap = assignment.tailGap.result,
                mergeableCount = assignment.midOverlap.size,
                mergeable = assignment.midOverlap.iterator,
                removeDeletes = removeDeletes,
                createdInLevel = createdInLevel,
                segmentParallelism = segmentParallelism,
                valuesConfig = valuesConfig,
                sortedIndexConfig = sortedIndexConfig,
                binarySearchIndexConfig = binarySearchIndexConfig,
                hashIndexConfig = hashIndexConfig,
                bloomFilterConfig = bloomFilterConfig,
                segmentConfig = segmentConfig
              )

            if (result.replaced) {
              singles ++= result.result
            } else {
              val merge = result.result :+ TransientSegment.Remote(fileHeader = Slice.emptyBytes, ref = assignment.segment)

              singles ++= merge.sortBy(_.minKey)(keyOrder)
            }
        }

        untouchedSegments foreach {
          oldRef =>
            singles += TransientSegment.Remote(fileHeader = Slice.emptyBytes, ref = oldRef)
        }

        if (tailGap.nonEmpty)
          singles ++=
            fastWriteOne(
              assignables = tailGap,
              createdInLevel = createdInLevel,
              valuesConfig = valuesConfig,
              sortedIndexConfig = sortedIndexConfig,
              binarySearchIndexConfig = binarySearchIndexConfig,
              hashIndexConfig = hashIndexConfig,
              bloomFilterConfig = bloomFilterConfig,
              segmentConfig = segmentConfig
            )

        val newMany =
          SegmentBlock.writeOneOrMany(
            createdInLevel = createdInLevel,
            ones = Slice.from(singles, singles.size),
            sortedIndexConfig = sortedIndexConfig,
            hashIndexConfig = hashIndexConfig,
            binarySearchIndexConfig = binarySearchIndexConfig,
            valuesConfig = valuesConfig,
            segmentConfig = segmentConfig
          )

        val newSegments =
          Segment.persistent(
            pathsDistributor = pathsDistributor,
            mmap = segmentConfig.mmap,
            createdInLevel = createdInLevel,
            transient = newMany
          )

        SegmentPutResult(result = newSegments, replaced = true)
      }
    }
  }

  def refresh(ref: SegmentRef,
              removeDeletes: Boolean,
              createdInLevel: Int,
              valuesConfig: ValuesBlock.Config,
              sortedIndexConfig: SortedIndexBlock.Config,
              binarySearchIndexConfig: BinarySearchIndexBlock.Config,
              hashIndexConfig: HashIndexBlock.Config,
              bloomFilterConfig: BloomFilterBlock.Config,
              segmentConfig: SegmentBlock.Config)(implicit keyOrder: KeyOrder[Slice[Byte]]): Slice[TransientSegment] = {
    val footer = ref.getFooter()
    val iterator = ref.iterator()
    //if it's created in the same level the required spaces for sortedIndex and values
    //will be the same as existing or less than the current sizes so there is no need to create a
    //MergeState builder.
    if (footer.createdInLevel == createdInLevel)
      Segment.refreshForSameLevel(
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
    else
      Segment.refreshForNewLevel(
        keyValues = iterator,
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

  def refreshPut(ref: SegmentRef,
                 removeDeletes: Boolean,
                 createdInLevel: Int,
                 valuesConfig: ValuesBlock.Config,
                 sortedIndexConfig: SortedIndexBlock.Config,
                 binarySearchIndexConfig: BinarySearchIndexBlock.Config,
                 hashIndexConfig: HashIndexBlock.Config,
                 bloomFilterConfig: BloomFilterBlock.Config,
                 segmentConfig: SegmentBlock.Config,
                 pathsDistributor: PathsDistributor)(implicit idGenerator: IDGenerator,
                                                     keyOrder: KeyOrder[Slice[Byte]],
                                                     timeOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore,
                                                     blockCacheSweeper: Option[MemorySweeper.Block],
                                                     fileSweeper: FileSweeper,
                                                     bufferCleaner: ByteBufferSweeperActor,
                                                     keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                     forceSaveApplier: ForceSaveApplier,
                                                     segmentIO: SegmentIO): Slice[PersistentSegment] = {

    val segments =
      SegmentRef.refresh(
        ref = ref,
        removeDeletes = removeDeletes,
        createdInLevel = createdInLevel,
        valuesConfig = valuesConfig,
        sortedIndexConfig = sortedIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        hashIndexConfig = hashIndexConfig,
        bloomFilterConfig = bloomFilterConfig,
        segmentConfig = segmentConfig
      )

    Segment.persistent(
      pathsDistributor = pathsDistributor,
      createdInLevel = createdInLevel,
      mmap = segmentConfig.mmap,
      transient = segments
    )
  }

  @inline def contains(key: Slice[Byte], ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    keyOrder.gteq(key, ref.minKey) && {
      if (ref.maxKey.inclusive)
        keyOrder.lteq(key, ref.maxKey.maxKey)
      else
        keyOrder.lt(key, ref.maxKey.maxKey)
    }

  @inline def containsLower(key: Slice[Byte], ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    keyOrder.gt(key, ref.minKey) && {
      if (ref.maxKey.inclusive)
        keyOrder.lteq(key, ref.maxKey.maxKey)
      else
        keyOrder.lt(key, ref.maxKey.maxKey)
    }

  @inline def containsHigher(key: Slice[Byte], ref: SegmentRef)(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    keyOrder.gteq(key, ref.minKey) && {
      if (ref.maxKey.inclusive)
        keyOrder.lteq(key, ref.maxKey.maxKey)
      else
        keyOrder.lt(key, ref.maxKey.maxKey)
    }
}

private[core] class SegmentRef(val path: Path,
                               val maxKey: MaxKey[Slice[Byte]],
                               val minKey: Slice[Byte],
                               val nearestPutDeadline: Option[Deadline],
                               val minMaxFunctionId: Option[MinMax[Slice[Byte]]],
                               val skipList: Option[SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent]],
                               val segmentBlockCache: SegmentBlockCache)(implicit keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                                         keyOrder: KeyOrder[Slice[Byte]]) extends SegmentRefOption with LazyLogging {

  implicit val self: SegmentRef = this
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))
  implicit val persistentKeyOrder: KeyOrder[Persistent] = KeyOrder(Ordering.by[Persistent, Slice[Byte]](_.key)(keyOrder))
  implicit val segmentSearcher: SegmentSearcher = SegmentSearcher

  override def isNoneC: Boolean =
    false

  override def getC: SegmentRef =
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

  private def applyToSkipList(f: SkipList[SliceOption[Byte], PersistentOption, Slice[Byte], Persistent] => PersistentOption): PersistentOption =
    if (skipList.isDefined)
      f(skipList.get)
    else
      Persistent.Null

  def getFromCache(key: Slice[Byte]): PersistentOption =
    skipList match {
      case Some(skipList) =>
        skipList get key

      case None =>
        Persistent.Null
    }

  def mightContainKey(key: Slice[Byte], threadState: ThreadReadState): Boolean = {
    val bloomFilterReader = segmentBlockCache.createBloomFilterReaderOrNull()
    bloomFilterReader == null ||
      BloomFilterBlock.mightContain(
        comparableKey = keyOrder.comparableKey(key),
        reader = bloomFilterReader
      )
  }

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

  def isInKeyValueCache(key: Slice[Byte]): Boolean =
    skipList.exists(_.contains(key))

  def cachedKeyValueSize: Int =
    skipList.foldLeft(0)(_ + _.size)

  def clearCachedKeyValues() =
    skipList.foreach(_.clear())

  def clearAllCaches() =
    segmentBlockCache.clear()

  def areAllCachesEmpty =
    isKeyValueCacheEmpty && !segmentBlockCache.isCached

  def readAllBytes(): Slice[Byte] =
    segmentBlockCache.readAllBytes()

  def segmentSize: Int =
    segmentBlockCache.segmentSize

  def get(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.get(key, threadState)

  def lower(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.lower(key, threadState)

  def higher(key: Slice[Byte], threadState: ThreadReadState): PersistentOption =
    SegmentRef.higher(key, threadState)

  def offset(): SegmentBlock.Offset =
    segmentBlockCache.offset()

  def blockCache(): Option[BlockCache.State] =
    segmentBlockCache.blockCache()

  override def equals(other: Any): Boolean =
    other match {
      case other: SegmentRef =>
        this.path == other.path

      case _ =>
        false
    }

  override def hashCode(): Int =
    path.hashCode()
}

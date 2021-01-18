/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.task

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.function.FunctionStore
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.merge.KeyValueMerger
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.core.util.Collections
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.{MaxKey, NonEmptyList}

import java.util
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

case object CompactionLevelZeroTasker {

  @inline def run(source: LevelZero,
                  lowerLevels: NonEmptyList[Level])(implicit ec: ExecutionContext): Future[CompactionTask.CompactMaps] = {
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder
    implicit val timeOrder: TimeOrder[Slice[Byte]] = source.timeOrder
    implicit val functionStore: FunctionStore = source.functionStore

    val (sourceIterator, processedMapsIterator) = source.maps.compactionIterator().duplicate

    flatten(sourceIterator) map {
      collections =>
        val tasks = CompactionTasker.run(
          data = collections,
          lowerLevels = lowerLevels,
          dataOverflow = Long.MaxValue //full overflow so that all collections are assigned and tasked.
        )

        CompactionTask.CompactMaps(
          targetLevel = source,
          maps = processedMapsIterator,
          tasks = tasks
        )
    }
  }

  @inline def flatten(input: Iterator[LevelZeroMap])(implicit ec: ExecutionContext,
                                                     keyOrder: KeyOrder[Slice[Byte]],
                                                     timerOrder: TimeOrder[Slice[Byte]],
                                                     functionStore: FunctionStore): Future[Iterable[Assignable.Collection]] =
    Future(createStacks(input)) flatMap {
      stacks =>
        Future.traverse(stacks.values().asScala.map(_.stack))(mergeStack) map {
          collection =>
            collection map {
              keyValues =>
                new Assignable.Collection {
                  override def key: Slice[Byte] =
                    keyValues.head.key

                  override def maxKey: MaxKey[Slice[Byte]] =
                    keyValues.last.maxKey

                  override def iterator(): Iterator[KeyValue] =
                    keyValues.iterator

                  override def keyValueCount: Int =
                    keyValues.size
                }
            }
        }
    }

  /**
   * Distributes [[LevelZeroMap]]s key-vales among themselves
   * so that concurrent merge could occur to flatten [[LevelZero]].
   */
  def createStacks(input: scala.collection.compat.IterableOnce[LevelZeroMap])(implicit keyOrder: KeyOrder[Slice[Byte]]): util.TreeMap[Slice[Byte], CompactionLevelZeroStack] = {
    import keyOrder._
    //builds a list of key-values that need to be merged
    //a stack looks like the following
    //
    //  (10 - 20) -> ListBuffer(KeyValues1 - 10,   12, 13, 15
    //                          KeyValues2 -   11, 12
    //                          KeyValues3 -   11,             20)

    val stacks = new util.TreeMap[Slice[Byte], CompactionLevelZeroStack](keyOrder)

    input foreach {
      inputMap =>
        val inputMinKey = inputMap.cache.skipList.headKey.getC
        val inputMaxKey = inputMap.cache.maxKey().getC
        //there could be range key-values in the Map so start with floor.
        //stacked - 10 - 20
        //input   -    15 - 30
        val floorStackedEntry = stacks.floorEntry(inputMinKey)

        val overlappingMinKey =
          if (floorStackedEntry == null)
            inputMinKey //there was no floor start from mapMinKey
          else
            floorStackedEntry.getValue.maxKey match {
              case MaxKey.Fixed(floorMaxKey) =>
                if (inputMinKey <= floorMaxKey)
                  floorStackedEntry.getKey
                else
                  inputMinKey

              case MaxKey.Range(_, floorMaxKey) =>
                if (inputMinKey < floorMaxKey)
                  floorStackedEntry.getKey
                else
                  inputMinKey
            }

        val overlappingStackedMaps = stacks.subMap(overlappingMinKey, true, inputMaxKey.maxKey, inputMaxKey.inclusive)

        if (overlappingStackedMaps.isEmpty) { //no overlap create a fresh entry
          val newStack =
            CompactionLevelZeroStack(
              minKey = inputMinKey,
              maxKey = inputMaxKey,
              stack = ListBuffer(Left(inputMap))
            )

          stacks.put(inputMinKey, newStack)
        } else { //distribute new key-values to their overlapping stacked maps.
          val existingMaps = overlappingStackedMaps.values().iterator().asScala

          //build the data to be updated.
          val updateEntries = ListBuffer.empty[(Slice[Byte], CompactionLevelZeroStack)]

          //iterate over all overlapping maps and slice the input and assign key-values to overlapping maps
          existingMaps.foldLeft((inputMinKey, true)) {
            case ((takeFrom, takeFromInclusive), existingStack) =>
              val inputKeyValues =
                if (existingMaps.hasNext)
                  inputMap.cache.skipList.subMapValues(
                    from = takeFrom,
                    fromInclusive = takeFromInclusive,
                    to = existingStack.maxKey.maxKey,
                    toInclusive = existingStack.maxKey.inclusive
                  )
                else //if it does not has next assign all to the last Map.
                  inputMap.cache.skipList.subMapValues(
                    from = takeFrom,
                    fromInclusive = takeFromInclusive,
                    to = inputMaxKey.maxKey,
                    toInclusive = true
                  )

              //this mutates existing stacks in the maps for update
              existingStack.stack += Right(inputKeyValues)

              val newMinKey =
                keyOrder.min(existingStack.minKey, inputKeyValues.head.key)

              val newMaxKey: MaxKey[Slice[Byte]] =
                existingStack.maxKey match { //existing maxKey
                  case MaxKey.Fixed(existingMaxKey) => //existing maxKey
                    val newMaxKey = inputKeyValues.last.maxKey //new maxKey
                    if (existingMaxKey >= newMaxKey.maxKey)
                      existingStack.maxKey
                    else
                      newMaxKey

                  case MaxKey.Range(_, existingMaxKey) =>
                    //since existing maxKey is a range if inputMap's maxKey is >= always select it
                    //because it could be a Fixed key-values.
                    val inputMapsMaxKey = inputKeyValues.last.maxKey
                    if (inputMapsMaxKey.maxKey >= existingMaxKey)
                      inputMapsMaxKey
                    else //else leave old.
                      existingStack.maxKey
                }

              //remove old entry & write new entry to set the mutated stack in the existing stack to the new key
              updateEntries += ((existingStack.minKey, existingStack.copy(minKey = newMinKey, maxKey = newMaxKey)))
              //if maxKey was inclusive then it should be exclusive
              //for next iteration because it's all been added by this iteration.
              (existingStack.maxKey.maxKey, !existingStack.maxKey.inclusive)
          }

          //finally remove all the updated maps and write new ones with new minKeys.
          updateEntries foreach {
            case (removeKey, newEntry) =>
              stacks.remove(removeKey)
              stacks.put(newEntry.minKey, newEntry)
          }
        }
    }

    //map over here will not be that expensive because stacks are generally small
    //if LevelZero is allowed 4 maps before compaction then this stack will have 4.
    stacks
  }

  private def getKeyValues(either: Either[LevelZeroMap, Iterable[Memory]]): Iterator[Memory] =
    either match {
      case Left(zeroMap) =>
        zeroMap.cache.valuesIterator()

      case Right(keyValues) =>
        keyValues.iterator
    }

  /**
   * Merges all input collections to form a single collection.
   */
  @inline def mergeStack(stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]])(implicit ec: ExecutionContext,
                                                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                                                  timerOrder: TimeOrder[Slice[Byte]],
                                                                                  functionStore: FunctionStore): Future[Iterable[Memory]] =
    if (stack.isEmpty)
      Futures.emptyIterable
    else if (Collections.hasOnlyOne(stack))
      stack.head match {
        case Left(map) =>
          Future.successful(map.cache.skipList.values())

        case Right(keyValues) =>
          Future.successful(keyValues)
      }
    else
      Future.traverse(stack.grouped(2).toList) {
        group =>
          if (group.size == 1)
            Future.successful(group.head)
          else
            Future {
              val newKeyValues = getKeyValues(group.head)
              val oldKeyValues = getKeyValues(group.last)

              val stats = MergeStats.buffer[Memory, ListBuffer](Aggregator.listBuffer)

              KeyValueMerger.merge(
                newKeyValues = newKeyValues,
                oldKeyValues = oldKeyValues,
                stats = stats,
                isLastLevel = false
              )

              Right(stats.result)
            }
      } flatMap {
        mergedResult =>
          mergeStack(mergedResult)
      }
}

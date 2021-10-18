/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level.compaction.task.assigner

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.function.FunctionStore
import swaydb.core.level.Level
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.level.compaction.task.CompactionTask.CompactMaps
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.merge.KeyValueMerger
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.Assignable
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.compaction.PushStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.{MaxKey, NonEmptyList}
import swaydb.utils.Futures

import java.util
import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

case object LevelZeroTaskAssigner {

  /**
   * Data type used to for flattening and merging [[swaydb.core.level.zero.LevelZero]].
   *
   * @param minKey smallest key in this stack
   * @param maxKey largest key in this stack
   * @param stack  overlapping data
   */
  case class Stack(minKey: Slice[Byte],
                   maxKey: MaxKey[Slice[Byte]],
                   stack: ListBuffer[Either[LevelZeroMap, Iterable[Memory]]])

  def run(source: LevelZero,
          pushStrategy: PushStrategy,
          lowerLevels: NonEmptyList[Level])(implicit ec: ExecutionContext,
                                            parallelism: CompactionParallelism): Future[CompactMaps] = {
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder
    implicit val timeOrder: TimeOrder[Slice[Byte]] = source.timeOrder
    implicit val functionStore: FunctionStore = source.functionStore

    //covert to List because now BehaviourCommit.commit requires
    //this maps to be reversible so do the conversion here instead
    //of working with iterators

    val mapsToCompact =
      List
        .from(source.maps.compactionIterator())
        .takeRight(source.mapsToCompact)

    flatten(mapsToCompact)
      .map {
        collections =>
          TaskAssigner.assignQuick(
            data = collections,
            lowerLevels = lowerLevels,
            dataOverflow = Long.MaxValue, //full overflow so that all collections are assigned and tasked.
            pushStrategy = pushStrategy
          )
      }
      .map {
        tasks =>
          CompactionTask.CompactMaps(
            source = source,
            maps = mapsToCompact,
            tasks = tasks
          )
      }
  }

  def flatten(input: IterableOnce[LevelZeroMap])(implicit ec: ExecutionContext,
                                                 keyOrder: KeyOrder[Slice[Byte]],
                                                 timerOrder: TimeOrder[Slice[Byte]],
                                                 functionStore: FunctionStore,
                                                 parallelism: CompactionParallelism): Future[Iterable[Assignable.Collection]] =
    Future(createStacks(input)) flatMap {
      stacks =>
        Futures.traverseBounded(parallelism.levelZeroFlattenParallelism, stacks.values().asScala.map(_.stack))(mergeStack) map {
          collection =>
            collection map {
              keyValues =>
                new Assignable.Collection {
                  override def key: Slice[Byte] =
                    keyValues.head.key

                  override def maxKey: MaxKey[Slice[Byte]] =
                    keyValues.last.maxKey

                  override def iterator(inOneSeek: Boolean): Iterator[KeyValue] =
                    keyValues.iterator

                  override def keyValueCount: Int =
                    keyValues.size
                }
            }
        }
    }

  /**
   * Distributes [[LevelZeroMap]]s key-values among themselves
   * to flatten the entire [[LevelZero]] so that it behaves like it was
   * a single [[Level]] without any conflicting key-values.
   *
   * This function does not perform any iterations on [[LevelZeroMap]]'s
   * key-values. It assigns based on the Map's head and last key-values.
   *
   * The resulting [[Stack]] will contains stacks
   * that can be merged concurrently before compacting [[LevelZeroMap]]s
   * onto lower [[Level]].
   *
   * @param input The [[LevelZeroMap]]s from [[LevelZero]] to compact.
   */
  def createStacks(input: IterableOnce[LevelZeroMap])(implicit keyOrder: KeyOrder[Slice[Byte]]): util.TreeMap[Slice[Byte], Stack] = {
    val stacks = new util.TreeMap[Slice[Byte], Stack](keyOrder)

    for (inputMap <- input)
      distributeMap(
        inputMap = inputMap,
        stacks = stacks
      )

    stacks
  }

  /**
   * @see [[createStacks]]'s function doc.
   */
  private def distributeMap(inputMap: LevelZeroMap,
                            stacks: util.NavigableMap[Slice[Byte], Stack])(implicit keyOrder: KeyOrder[Slice[Byte]]): Unit = {
    import keyOrder._
    //builds a list of key-values that need to be merged
    //a stack looks like the following
    //
    //  (10 - 20) -> ListBuffer(KeyValues1 - 10,   12, 13, 15
    //                          KeyValues2 -   11, 12
    //                          KeyValues3 -   11,             20)

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

    //fetches existing maps that are overlapping the new inputMap
    @inline def getOverlappingExistingMaps() = stacks.subMap(overlappingMinKey, true, inputMaxKey.maxKey, inputMaxKey.inclusive)

    //mutable - which gets reset if the inputMap had range key-values which spreads to multiple maps.
    var overlappingExistingMaps = getOverlappingExistingMaps()

    if (overlappingExistingMaps.isEmpty) { //IF - no overlaps create a fresh entry
      val newStack =
        Stack(
          minKey = inputMinKey,
          maxKey = inputMaxKey,
          stack = ListBuffer(Left(inputMap))
        )

      stacks.put(inputMinKey, newStack)
    } else { //ELSE distribute new key-values to their overlapping stacked maps.
      /**
       * Before distributing check if inputMap has range that spreads to multiple existing Maps.
       * If yes, then combine those existing maps before distributing data because we don't want
       * perform range splitting here. To keep it simple we simply join the existing conflicting
       * maps and defer the split to SegmentAssigner which occurs later in the merge process.
       */
      if (inputMap.cache.hasRange && overlappingExistingMaps.size() > 1) {
        var minKey: Slice[Byte] = null
        var maxKey: MaxKey[Slice[Byte]] = null
        //overlapping existing stacks to join to form a single CompactionLevelZeroStack
        val joinedStack = ListBuffer.empty[Either[LevelZeroMap, Iterable[Memory]]]

        overlappingExistingMaps.values().iterator() forEachRemaining {
          stack =>
            if (minKey == null) minKey = stack.minKey
            maxKey = stack.maxKey
            joinedStack ++= stack.stack
        }

        //remove existing overlapping maps
        overlappingExistingMaps.clear()

        //create the new stack
        val newCollapsedStack =
          Stack(
            minKey = minKey,
            maxKey = maxKey,
            stack = joinedStack
          )

        //insert the stack after clearing old assignments.
        stacks.put(minKey, newCollapsedStack)

        //reset the overlapping maps. floorStackedEntry does not require reset because the floorKey is still the same.
        overlappingExistingMaps = getOverlappingExistingMaps()
        //this time there is only one overlapping
        assert(overlappingExistingMaps.size() == 1, s"${overlappingExistingMaps.size()} != 1")
      }

      /** ***********************
       * START DATA DISTRIBUTION.
       * ********************** */
      val existingMaps = overlappingExistingMaps.values().iterator().asScala
      //build the data to be updated.
      val newUpdatedStacks = ListBuffer.empty[Stack]

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
          newUpdatedStacks += existingStack.copy(minKey = newMinKey, maxKey = newMaxKey)
          //if maxKey was inclusive then it should be exclusive
          //for next iteration because it's all been added by this iteration.
          (existingStack.maxKey.maxKey, !existingStack.maxKey.inclusive)
      }

      //clear old assignments
      overlappingExistingMaps.clear()

      //finally remove all the updated maps and write new ones with new minKeys.
      newUpdatedStacks foreach {
        newEntry =>
          stacks.put(newEntry.minKey, newEntry)
      }
    }
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
  def mergeStack(stack: Iterable[Either[LevelZeroMap, Iterable[Memory]]])(implicit ec: ExecutionContext,
                                                                          keyOrder: KeyOrder[Slice[Byte]],
                                                                          timerOrder: TimeOrder[Slice[Byte]],
                                                                          functionStore: FunctionStore,
                                                                          parallelism: CompactionParallelism): Future[Iterable[Memory]] =
    if (stack.isEmpty)
      Futures.emptyIterable
    else if (stack.size == 1)
      stack.head match {
        case Left(map) =>
          Future.successful(map.cache.skipList.values())

        case Right(keyValues) =>
          Future.successful(keyValues)
      }
    else
      Futures.traverseBounded(parallelism.levelZeroMergeParallelism, stack.grouped(2).toList) {
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
                isLastLevel = false,
                //LevelZero key-values are already in-memory so this does not make any difference
                initialiseIteratorsInOneSeek = false
              )

              Right(stats.result)
            }
      } flatMap {
        mergedResult =>
          mergeStack(mergedResult)
      }
}

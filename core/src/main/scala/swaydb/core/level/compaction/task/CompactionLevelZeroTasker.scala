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
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.{MaxKey, NonEmptyList}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

case object CompactionLevelZeroTasker {

  @inline def run(source: LevelZero,
                  lowerLevels: NonEmptyList[Level])(implicit ec: ExecutionContext): Future[CompactionTask.CompactMaps] = {
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder
    implicit val timeOrder: TimeOrder[Slice[Byte]] = source.timeOrder
    implicit val functionStore: FunctionStore = source.functionStore

    val (sourceIterator, processedMapsIterator) = source.maps.compactionIterator.duplicate

    flatten(sourceIterator) map {
      collections =>
        val tasks = CompactionTasker.run(
          data = collections,
          lowerLevels = lowerLevels,
          dataOverflow = Int.MaxValue //full overflow so that all collections are assigned and tasks.
        )

        CompactionTask.CompactMaps(
          levelZero = source,
          maps = processedMapsIterator.toList,
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
        Future.traverse(stacks)(mergeStack) map {
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

  def createStacks(input: Iterator[LevelZeroMap])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[ListBuffer[Either[LevelZeroMap, Iterable[Memory]]]] = {
    //value are tables values where the first ListBuffer is row and inner are columns.
    val stacks = new java.util.TreeMap[Slice[Byte], (MaxKey[Slice[Byte]], ListBuffer[Either[LevelZeroMap, Iterable[Memory]]])](keyOrder)

    input foreach {
      map =>
        val mapMinKey = map.cache.skipList.headKey.getC
        val mapMaxKey = map.cache.maxKey().getC
        val overlappingMaps = stacks.subMap(mapMinKey, true, mapMaxKey.maxKey, mapMaxKey.inclusive)

        if (overlappingMaps.isEmpty) { //no overlap create a fresh entry
          stacks.put(mapMinKey, (mapMaxKey, ListBuffer(Left(map))))
        } else { //distribute new key-values to their overlapping stacked maps.
          val valuesIterator = overlappingMaps.values().iterator().asScala

          valuesIterator.foldLeft((mapMinKey, true)) {
            case ((takeFrom, fromInclusive), (overlappingMaxKey, stack)) =>
              val overlappingKeyValues =
                if (valuesIterator.hasNext)
                  map.cache.skipList.subMap(
                    from = takeFrom,
                    fromInclusive = fromInclusive,
                    to = overlappingMaxKey.maxKey,
                    toInclusive = overlappingMaxKey.inclusive
                  )
                else //if it does not has next assign all.
                  map.cache.skipList.subMap(
                    from = takeFrom,
                    fromInclusive = fromInclusive,
                    to = mapMaxKey.maxKey,
                    toInclusive = true
                  )

              stack += Right(overlappingKeyValues.map(_._2))

              (overlappingMaxKey.maxKey, !overlappingMaxKey.inclusive)
          }
        }
    }

    stacks.values().asScala.map(_._2)
  }

  private def getKeyValues(either: Either[LevelZeroMap, Iterable[Memory]]): (Int, Iterator[Memory]) =
    either match {
      case Left(value) =>
        (value.cache.maxKeyValueCount, value.cache.iterator.map(_._2))

      case Right(value) =>
        (value.size, value.iterator)
    }

  @inline def mergeStack(collections: Iterable[Either[LevelZeroMap, Iterable[Memory]]])(implicit ec: ExecutionContext,
                                                                                        keyOrder: KeyOrder[Slice[Byte]],
                                                                                        timerOrder: TimeOrder[Slice[Byte]],
                                                                                        functionStore: FunctionStore): Future[Iterable[Memory]] =
    if (collections.size == 1)
      collections.head match {
        case Left(map) =>
          Future.successful(map.cache.skipList.values())

        case Right(keyValues) =>
          Future.successful(keyValues)
      }
    else
      Future.traverse(collections.grouped(2).toList) {
        group =>
          if (group.size == 1)
            Future.successful(group.head)
          else
            Future {
              val (newKeyValuesCount, newKeyValues) = getKeyValues(group.head)
              val (oldKeyValuesCount, oldKeyValues) = getKeyValues(group.last)

              val stats = MergeStats.buffer[Memory, ListBuffer](Aggregator.listBuffer)

              KeyValueMerger.merge(
                newKeyValuesCount = newKeyValuesCount,
                newKeyValues = newKeyValues,
                oldKeyValuesCount = oldKeyValuesCount,
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

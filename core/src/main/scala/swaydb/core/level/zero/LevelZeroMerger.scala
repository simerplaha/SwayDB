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

package swaydb.core.level.zero

import swaydb.core.level.zero.LevelZeroLogCache.State
import swaydb.core.segment.CoreFunctionStore
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.data.merge.{FixedMerger, KeyValueMerger}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.Aggregator

import scala.collection.mutable.ListBuffer

object LevelZeroMerger {

  @inline def mergeInsert(insert: Memory,
                          state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: CoreFunctionStore): Unit =
    insert match {
      case insert: Memory.Fixed =>
        insertFixed(insert = insert, state = state)

      case insert: Memory.Range =>
        insertRange(insert = insert, state = state)
    }

  /**
   * Inserts a [[Memory.Fixed]] key-value into skipList.
   */
  @inline private def insertFixed(insert: Memory.Fixed,
                                  state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: CoreFunctionStore): Unit =
    state.skipList.floor(insert.key) match {
      case floorEntry: Memory =>
        import keyOrder._

        floorEntry match {
          //if floor entry for input Fixed entry & if they keys match, do applyValue else simply add the new key-value.
          case floor: Memory.Fixed if floor.key equiv insert.key =>
            val mergedKeyValue =
              FixedMerger(
                newKeyValue = insert,
                oldKeyValue = floor
              ).asInstanceOf[Memory.Fixed]

            if (!mergedKeyValue.isPut) state.setHasNonPutToTrue()
            state.skipList.put(insert.key, mergedKeyValue)

          //if the floor entry is a range try to do a merge.
          case floorRange: Memory.Range if insert.key < floorRange.toKey =>

            val builder = MergeStats.buffer[Memory, ListBuffer](Aggregator.listBuffer)

            KeyValueMerger.merge(
              newKeyValue = insert,
              oldKeyValue = floorRange,
              builder = builder,
              isLastLevel = false,
              initialiseIteratorsInOneSeek = false
            )

            val mergedKeyValues = builder.keyValues

            mergedKeyValues foreach {
              merged: Memory =>
                if (merged.isRange) state.setHasRangeToTrue()
                if (!merged.isPut) state.setHasNonPutToTrue()
                state.skipList.put(merged.key, merged)
            }

          case _ =>
            if (!insert.isPut) state.setHasNonPutToTrue()
            state.skipList.put(insert.key, insert)
        }

      //if there is no floor, simply put.
      case Memory.Null =>
        if (!insert.isPut) state.setHasNonPutToTrue()
        state.skipList.put(insert.key, insert)
    }

  /**
   * Inserts the input [[Memory.Range]] key-value into skipList and always maintaining the previous state of
   * the skipList before applying the new state so that all read queries read the latest write.
   */
  @inline private def insertRange(insert: Memory.Range,
                                  state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: CoreFunctionStore): Unit = {
    import keyOrder._

    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      state.skipList.floor(insert.fromKey) mapS {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = state.skipList.subMap(from = startKey, fromInclusive = true, to = insert.toKey, toInclusive = false)
    if (conflictingKeyValues.isEmpty) {
      state.setHasRangeToTrue() //set this before put so reads know to floor this skipList.
      state.skipList.put(insert.key, insert)
    } else {
      val oldKeyValues = Slice.allocate[Memory](conflictingKeyValues.size)

      conflictingKeyValues foreach {
        case (_, keyValue) =>
          oldKeyValues add keyValue
      }

      val builder = MergeStats.buffer[Memory, ListBuffer](Aggregator.fromBuilder(ListBuffer.newBuilder))

      KeyValueMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        stats = builder,
        isLastLevel = false,
        initialiseIteratorsInOneSeek = false
      )

      val mergedKeyValues = builder.keyValues

      state.setHasRangeToTrue() //set this before put so reads know to floor this skipList.

      oldKeyValues foreach {
        oldKeyValue =>
          state.skipList.remove(oldKeyValue.key)
      }

      mergedKeyValues foreach {
        keyValue =>
          state.skipList.put(keyValue.key, keyValue)
      }
    }
  }

}

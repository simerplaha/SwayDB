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

package swaydb.core.level.zero

import swaydb.Aggregator
import swaydb.core.data.Memory
import swaydb.core.function.FunctionStore
import swaydb.core.level.zero.LevelZeroMapCache.State
import swaydb.core.merge.stats.MergeStats
import swaydb.core.merge.{FixedMerger, KeyValueMerger}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

object LevelZeroMerger {

  @inline def mergeInsert(insert: Memory,
                          state: State)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        functionStore: FunctionStore): Unit =
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
                                                functionStore: FunctionStore): Unit =
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
              isLastLevel = false
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
                                                functionStore: FunctionStore) = {
    import keyOrder._

    //value the start position of this range to fetch the range's start and end key-values for the skipList.
    val startKey =
      state.skipList.floor(insert.fromKey) mapS {
        case range: Memory.Range if insert.fromKey < range.toKey =>
          range.fromKey

        case _ =>
          insert.fromKey
      } getOrElse insert.fromKey

    val conflictingKeyValues = state.skipList.subMap(startKey, true, insert.toKey, false)
    if (conflictingKeyValues.isEmpty) {
      state.setHasRangeToTrue() //set this before put so reads know to floor this skipList.
      state.skipList.put(insert.key, insert)
    } else {
      val oldKeyValues = Slice.of[Memory](conflictingKeyValues.size)

      conflictingKeyValues foreach {
        case (_, keyValue) =>
          oldKeyValues add keyValue
      }

      val builder = MergeStats.buffer[Memory, ListBuffer](Aggregator.fromBuilder(ListBuffer.newBuilder))

      KeyValueMerger.merge(
        newKeyValues = Slice(insert),
        oldKeyValues = oldKeyValues,
        stats = builder,
        isLastLevel = false
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

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

package swaydb.core.merge

import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object FixedMerger {

  def apply(newer: KeyValue.Fixed,
            older: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed =
    FixedMerger(
      newer = newer,
      oldApplies = older.getOrFetchApplies
    )

  def apply(newer: KeyValue.Fixed,
            oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): KeyValue.Fixed =
    oldApplies.reverse.foldLeft((newer, 0)) {
      case ((newerMerged, count), olderApply) =>
        newerMerged match {
          case newer: KeyValue.Put =>
            val merged = PutMerger(newer, olderApply)
            (merged, count + 1)

          case newer: KeyValue.Function =>
            val merged = FunctionMerger(newer, olderApply)
            (merged, count + 1)

          case newer: KeyValue.Remove =>
            val merged = RemoveMerger(newer, olderApply)
            (merged, count + 1)

          case newer: KeyValue.Update =>
            val merged = UpdateMerger(newer, olderApply)
            (merged, count + 1)

          case newer: KeyValue.PendingApply =>
            return {
              val newerApplies = newer.getOrFetchApplies
              val newMergedApplies = oldApplies.dropRight(count) ++ newerApplies
              if (newMergedApplies.size == 1)
                newMergedApplies.head.toMemory(newer.key)
              else
                Memory.PendingApply(newer.key, newMergedApplies)
            }
        }
    }._1

  def apply(newKeyValue: KeyValue.Fixed,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): KeyValue.Fixed =
    newKeyValue match {
      case newKeyValue: KeyValue.Put =>
        PutMerger(newKeyValue, oldKeyValue)

      case newKeyValue: KeyValue.Remove =>
        RemoveMerger(newKeyValue, oldKeyValue)

      case newKeyValue: KeyValue.Function =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case newKeyValue: KeyValue.Update =>
        UpdateMerger(newKeyValue, oldKeyValue)

      case newKeyValue: KeyValue.PendingApply =>
        PendingApplyMerger(newKeyValue, oldKeyValue)
    }
}

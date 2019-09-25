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

package swaydb.core.merge

import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object FixedMerger {

  def apply(newer: ReadOnly.Fixed,
            older: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): ReadOnly.Fixed =
    FixedMerger(
      newer = newer,
      oldApplies = older.getOrFetchApplies
    )

  def apply(newer: ReadOnly.Fixed,
            oldApplies: Slice[Value.Apply])(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): ReadOnly.Fixed =
    oldApplies.reverse.toIterable.foldLeft((newer, 0)) {
      case ((newerMerged, count), olderApply) =>
        newerMerged match {
          case newer: ReadOnly.Put =>
            val merged = PutMerger(newer, olderApply)
            (merged, count + 1)

          case newer: ReadOnly.Function =>
            val merged = FunctionMerger(newer, olderApply)
            (merged, count + 1)

          case newer: ReadOnly.Remove =>
            val merged = RemoveMerger(newer, olderApply)
            (merged, count + 1)

          case newer: ReadOnly.Update =>
            val merged = UpdateMerger(newer, olderApply)
            (merged, count + 1)

          case newer: ReadOnly.PendingApply =>
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

  def apply(newKeyValue: ReadOnly.Fixed,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): ReadOnly.Fixed =
    newKeyValue match {
      case newKeyValue: ReadOnly.Put =>
        PutMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.Remove =>
        RemoveMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.Function =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.Update =>
        UpdateMerger(newKeyValue, oldKeyValue)

      case newKeyValue: ReadOnly.PendingApply =>
        PendingApplyMerger(newKeyValue, oldKeyValue)
    }
}

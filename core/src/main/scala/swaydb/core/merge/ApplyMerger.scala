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

package swaydb.core.merge

import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object ApplyMerger {

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: KeyValue.Put)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      oldKeyValue

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      oldKeyValue

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: KeyValue.Update)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      oldKeyValue

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      oldKeyValue

  def apply(newKeyValue: Value.Apply,
            oldKeyValue: KeyValue.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue match {
        case newValue: Value.Remove =>
          RemoveMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Update =>
          UpdateMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)

        case newValue: Value.Function =>
          FunctionMerger(newValue.toMemory(oldKeyValue.key), oldKeyValue)
      }
    else
      oldKeyValue

  def apply(newApplies: Slice[Value.Apply],
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): KeyValue.Fixed =
    newApplies.foldLeft((oldKeyValue, 0)) {
      case ((oldMerged, count), newApply) =>
        oldMerged match {
          case old: KeyValue.Put =>
            val merged = ApplyMerger(newApply, old)
            (merged, count + 1)

          case old: KeyValue.Remove =>
            val merged = ApplyMerger(newApply, old)
            (merged, count + 1)

          case old: KeyValue.Function =>
            val merged = ApplyMerger(newApply, old)
            (merged, count + 1)

          case old: KeyValue.Update =>
            val merged = ApplyMerger(newApply, old)
            (merged, count + 1)

          case old: KeyValue.PendingApply =>
            return {
              val oldMergedApplies = old.getOrFetchApplies
              val resultApplies = oldMergedApplies ++ newApplies.drop(count)
              if (resultApplies.size == 1)
                resultApplies.head.toMemory(oldKeyValue.key)
              else
                Memory.PendingApply(old.key, resultApplies)
            }
        }
    }._1
}

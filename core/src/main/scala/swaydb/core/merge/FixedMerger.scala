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

package swaydb.core.merge

import swaydb.core.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.slice.Slice

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

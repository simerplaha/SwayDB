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

package swaydb.core.segment.data.merge

import swaydb.core.segment.data.{KeyValue, Memory, Value}
import swaydb.core.function.FunctionStore
import swaydb.slice.order.TimeOrder
import swaydb.slice.Slice

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

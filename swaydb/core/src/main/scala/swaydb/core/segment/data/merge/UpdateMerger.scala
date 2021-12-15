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

import swaydb.core.segment.CoreFunctionStore
import swaydb.core.segment.data.{KeyValue, Memory, Value}
import swaydb.slice.Slice
import swaydb.slice.order.TimeOrder

private[core] object UpdateMerger {

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          newKeyValue.toPut()

        case (Some(_), None) =>
          newKeyValue.toPut()

        case (None, Some(_)) =>
          newKeyValue.toPut(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue.toPut()
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case (Some(_), None) =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case (None, Some(_)) =>
          newKeyValue.copyWithDeadline(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Update =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, None) =>
          newKeyValue

        case (Some(_), None) =>
          newKeyValue

        case (None, Some(_)) =>
          newKeyValue.copyWithDeadline(oldKeyValue.deadline)

        case (Some(_), Some(_)) =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time) {
      val oldValue = oldKeyValue.toFromValue()
      val newValue = newKeyValue.toFromValue()
      Memory.PendingApply(newKeyValue.key, Slice(oldValue, newValue))
    }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: Value.Remove =>
          UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))

        case oldKeyValue: Value.Update =>
          UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))

        case oldKeyValue: Value.Function =>
          UpdateMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
      }
    else
      oldKeyValue.toMemory(newKeyValue.key)

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: CoreFunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      FixedMerger(
        newer = newKeyValue,
        oldApplies = oldKeyValue.getOrFetchApplies
      )
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Update,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: CoreFunctionStore): KeyValue.Fixed =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: KeyValue.Put =>             UpdateMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Remove =>          UpdateMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Update =>          UpdateMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Function =>        UpdateMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.PendingApply =>    UpdateMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}

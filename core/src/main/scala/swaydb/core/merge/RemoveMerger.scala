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

private[core] object RemoveMerger {

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Remove =
    if (newKeyValue.time > oldKeyValue.time)
      (newKeyValue.deadline, oldKeyValue.deadline) match {
        case (None, _) =>
          newKeyValue

        case (Some(_), None) =>
          oldKeyValue.copyWithTime(time = newKeyValue.time)

        case (Some(_), Some(_)) =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: KeyValue.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.deadline match {
        case Some(_) =>
          oldKeyValue.copyWithDeadlineAndTime(newKeyValue.deadline, newKeyValue.time)

        case None =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: KeyValue.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.deadline match {
        case Some(_) =>
          oldKeyValue.copyWithDeadlineAndTime(newKeyValue.deadline, newKeyValue.time)

        case None =>
          newKeyValue
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: KeyValue.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.deadline match {
        case None =>
          newKeyValue

        case Some(_) =>
          Memory.PendingApply(
            key = newKeyValue.key,
            applies = Slice(oldKeyValue.toFromValue(), newKeyValue.toRemoveValue())
          )
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: Value.Remove =>
          RemoveMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))

        case oldKeyValue: Value.Update =>
          RemoveMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))

        case oldKeyValue: Value.Function =>
          RemoveMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
      }
    else
      oldKeyValue.toMemory(newKeyValue.key)

  def apply(newer: KeyValue.Remove,
            older: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed =
    if (newer.time > older.time)
      newer.deadline match {
        case Some(_) =>
          FixedMerger(
            newer = newer,
            oldApplies = older.getOrFetchApplies
          )

        case None =>
          newer
      }
    else
      older

  def apply(newKeyValue: KeyValue.Remove,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): KeyValue.Fixed =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: KeyValue.Put =>             RemoveMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Remove =>          RemoveMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Update =>          RemoveMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Function =>        RemoveMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.PendingApply =>    RemoveMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}

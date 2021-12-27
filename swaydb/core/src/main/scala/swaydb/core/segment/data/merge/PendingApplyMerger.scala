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
import swaydb.core.segment.data.{KeyValue, Value}
import swaydb.slice.Slice
import swaydb.slice.order.TimeOrder

private[core] object PendingApplyMerger {

  /**
   * PendingApply
   */
  def apply(newKeyValue: KeyValue.PendingApply,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: CoreFunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: KeyValue.Remove if oldKeyValue.deadline.isEmpty =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case _: KeyValue.Fixed =>
          ApplyMerger(
            newApplies = newKeyValue.getOrFetchApplies(),
            oldKeyValue = oldKeyValue
          )
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.PendingApply,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: CoreFunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue.deadline match {
        case None =>
          oldKeyValue

        case Some(_) =>
          PendingApplyMerger(newKeyValue, oldKeyValue: KeyValue.Fixed)
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.PendingApply,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: CoreFunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      PendingApplyMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
    else
      oldKeyValue.toMemory(newKeyValue.key)
}

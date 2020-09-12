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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.merge

import swaydb.core.data.KeyValue
import swaydb.core.data.Value
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice._
private[core] object PendingApplyMerger {

  /**
   * PendingApply
   */
  def apply(newKeyValue: KeyValue.PendingApply,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: KeyValue.Remove if oldKeyValue.deadline.isEmpty =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case _: KeyValue.Fixed =>
          ApplyMerger(
            newApplies = newKeyValue.getOrFetchApplies,
            oldKeyValue = oldKeyValue
          )
      }
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.PendingApply,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed =
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
                                      functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      PendingApplyMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
    else
      oldKeyValue.toMemory(newKeyValue.key)
}

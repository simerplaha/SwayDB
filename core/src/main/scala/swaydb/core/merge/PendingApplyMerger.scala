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

import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.Value
import swaydb.core.function.FunctionStore
import swaydb.data.io.Core
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice
import swaydb.data.io.Core.Error.ErrorHandler

private[core] object PendingApplyMerger {

  /**
    * PendingApply
    */
  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): IO[Core.Error, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue match {
        case oldKeyValue: ReadOnly.Remove if oldKeyValue.deadline.isEmpty =>
          IO.Success(oldKeyValue.copyWithTime(newKeyValue.time))

        case _: ReadOnly.Fixed =>
          newKeyValue.getOrFetchApplies flatMap {
            newApplies =>
              ApplyMerger(newApplies, oldKeyValue)
          }
      }
    else
      IO.Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): IO[Core.Error, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue.deadline match {
        case None =>
          IO.Success(oldKeyValue)

        case Some(_) =>
          PendingApplyMerger(newKeyValue, oldKeyValue: ReadOnly.Fixed)
      }
    else
      IO.Success(oldKeyValue)

  def apply(newKeyValue: ReadOnly.PendingApply,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[Core.Error, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      PendingApplyMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key))
    else
      IO.Success(oldKeyValue.toMemory(newKeyValue.key))
}

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
import swaydb.core.data.Value
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

object PutMerger {

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Put =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue.toMemory(newKeyValue.key)

  def apply(newKeyValue: ReadOnly.Put,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]]): ReadOnly.Fixed =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: ReadOnly.Put =>             PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Remove =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Update =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.Function =>        PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: ReadOnly.PendingApply =>    PutMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}

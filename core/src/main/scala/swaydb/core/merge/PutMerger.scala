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
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice
private[core] object PutMerger {

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.Put)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Put =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.Update)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue
    else
      oldKeyValue.toMemory(newKeyValue.key)

  def apply(newKeyValue: KeyValue.Put,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
  //@formatter:off
    oldKeyValue match {
      case oldKeyValue: KeyValue.Put =>             PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Remove =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Update =>          PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.Function =>        PutMerger(newKeyValue, oldKeyValue)
      case oldKeyValue: KeyValue.PendingApply =>    PutMerger(newKeyValue, oldKeyValue)
    }
  //@formatter:on

}

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

import swaydb.core.data.Value
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._

private[core] object ValueMerger {

  def apply(key: Slice[Byte],
            newRangeValue: Value.RangeValue,
            oldFromValue: Value.FromValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): Value.FromValue =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(key),
      oldKeyValue = oldFromValue.toMemory(key)
    ).toFromValue()

  def apply(key: Slice[Byte],
            newRangeValue: Value.FromValue,
            oldFromValue: Value.FromValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): Value.FromValue =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(key),
      oldKeyValue = oldFromValue.toMemory(key)
    ).toFromValue()

  def apply(newRangeValue: Value.RangeValue,
            oldRangeValue: Value.RangeValue)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                             functionStore: FunctionStore): Value.RangeValue =
    FixedMerger(
      newKeyValue = newRangeValue.toMemory(Slice.emptyBytes),
      oldKeyValue = oldRangeValue.toMemory(Slice.emptyBytes)
    ).toRangeValue()
}

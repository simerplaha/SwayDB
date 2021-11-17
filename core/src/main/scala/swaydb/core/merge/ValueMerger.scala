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

import swaydb.core.segment.data.Value
import swaydb.core.function.FunctionStore
import swaydb.slice.order.TimeOrder
import swaydb.slice.Slice

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

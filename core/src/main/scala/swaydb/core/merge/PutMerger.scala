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

import swaydb.core.data.{KeyValue, Value}
import swaydb.data.order.TimeOrder
import swaydb.slice.Slice

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

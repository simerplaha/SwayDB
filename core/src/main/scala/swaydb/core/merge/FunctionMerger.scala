/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.merge

import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object FunctionMerger {

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.Put)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore): KeyValue.Fixed = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          oldKeyValue.copyWithDeadlineAndTime(Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Put(oldKeyValue.key, value.asSliceOption(), deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    if (newKeyValue.time > oldKeyValue.time) {
      val function = newKeyValue.getOrFetchFunction
      functionStore.get(function) match {
        case Some(functionId) =>
          functionId match {
            case SwayFunction.Value(f) =>
              val output = f(oldKeyValue.getOrFetchValue)
              applyOutput(output)

            case SwayFunction.ValueDeadline(f) =>
              val value = oldKeyValue.getOrFetchValue
              val output = f(value, oldKeyValue.deadline)
              applyOutput(output)

            case function: SwayFunction.RequiresKey =>
              function match {
                case SwayFunction.Key(f) =>
                  applyOutput(f(oldKeyValue.key))

                case SwayFunction.KeyValue(f) =>
                  val oldValue = oldKeyValue.getOrFetchValue
                  val output = f(oldKeyValue.key, oldValue)
                  applyOutput(output)

                case SwayFunction.KeyDeadline(f) =>
                  val output = f(oldKeyValue.key, oldKeyValue.deadline)
                  applyOutput(output)

                case SwayFunction.KeyValueDeadline(f) =>
                  val oldValue = oldKeyValue.getOrFetchValue
                  val output = f(oldKeyValue.key, oldValue, oldKeyValue.deadline)
                  applyOutput(output)
              }
          }

        case None =>
          throw swaydb.Exception.FunctionNotFound(function.readString())
      }
    }

    else
      oldKeyValue
  }

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.Update)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          oldKeyValue.copyWithDeadlineAndTime(Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Update(oldKeyValue.key, value.asSliceOption(), deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    def toPendingApply(): Memory.PendingApply = {
      val oldValue = oldKeyValue.toFromValue()
      val newValue = newKeyValue.toFromValue()
      Memory.PendingApply(oldKeyValue.key, Slice(oldValue, newValue))
    }

    if (newKeyValue.time > oldKeyValue.time) {
      val function = newKeyValue.getOrFetchFunction
      functionStore.get(function) match {
        case Some(functionId) =>
          functionId match {
            case SwayFunction.Value(f) =>
              val value = oldKeyValue.getOrFetchValue
              applyOutput(f(value))

            case SwayFunction.ValueDeadline(f) =>
              //if deadline is not set, then the deadline of this key might have another update in lower levels.
              //so stash update.
              if (oldKeyValue.deadline.isEmpty) {
                toPendingApply()
              } else {
                val value = oldKeyValue.getOrFetchValue
                val output = f(value, oldKeyValue.deadline)
                applyOutput(output)
              }

            case function: SwayFunction.RequiresKey =>
              if (oldKeyValue.key.isEmpty)
                toPendingApply()
              else
                function match {
                  case SwayFunction.Key(f) =>
                    applyOutput(f(oldKeyValue.key))

                  case SwayFunction.KeyValue(f) =>
                    val oldValue = oldKeyValue.getOrFetchValue
                    val output = f(oldKeyValue.key, oldValue)
                    applyOutput(output)

                  case SwayFunction.KeyDeadline(f) =>
                    if (oldKeyValue.deadline.isEmpty)
                      toPendingApply()
                    else
                      applyOutput(f(oldKeyValue.key, oldKeyValue.deadline))

                  case SwayFunction.KeyValueDeadline(f) =>
                    if (oldKeyValue.deadline.isEmpty) {
                      toPendingApply()
                    } else {
                      val oldValue = oldKeyValue.getOrFetchValue
                      val output = f(oldKeyValue.key, oldValue, oldKeyValue.deadline)
                      applyOutput(output)
                    }
                }
          }

        case None =>
          throw swaydb.Exception.FunctionNotFound(function.readString())
      }
    }
    else
      oldKeyValue
  }

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): KeyValue.Fixed = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          Memory.Remove(oldKeyValue.key, Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Update(oldKeyValue.key, value.asSliceOption(), deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    def toPendingApply() =
      Memory.PendingApply(
        key = oldKeyValue.key,
        applies = Slice(oldKeyValue.toRemoveValue(), newKeyValue.toFromValue())
      )

    if (newKeyValue.time > oldKeyValue.time) {
      val function = newKeyValue.getOrFetchFunction
      oldKeyValue.deadline match {
        case None =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case Some(_) =>
          functionStore.get(function) match {
            case Some(function) =>
              function match {
                case _: SwayFunction.RequiresKey if oldKeyValue.key.isEmpty =>
                  //key is unknown since it's empty. Stash the merge.
                  toPendingApply()

                case _: SwayFunction.RequiresValue =>
                  //value is not known since remove has deadline set - PendingApply!
                  toPendingApply()

                case SwayFunction.Key(f) =>
                  applyOutput(f(oldKeyValue.key))

                case SwayFunction.KeyDeadline(f) =>
                  applyOutput(f(oldKeyValue.key, oldKeyValue.deadline))
              }

            case None =>
              throw swaydb.Exception.FunctionNotFound(function.readString())
          }
      }
    }

    else
      oldKeyValue
  }

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.Function)(implicit timeOrder: TimeOrder[Slice[Byte]]): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      Memory.PendingApply(
        key = newKeyValue.key,
        applies = Slice(oldKeyValue.toFromValue(), newKeyValue.toFromValue())
      )
    else
      oldKeyValue

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): KeyValue.Fixed =
    oldKeyValue match {
      case oldKeyValue: KeyValue.Put =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: KeyValue.Remove =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: KeyValue.Update =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: KeyValue.Function =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: KeyValue.PendingApply =>
        FunctionMerger(newKeyValue, oldKeyValue)
    }

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): KeyValue.Fixed =
    oldKeyValue match {
      case oldKeyValue: Value.Remove =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): KeyValue.Fixed)

      case oldKeyValue: Value.Update =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): KeyValue.Fixed)

      case oldKeyValue: Value.Function =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): KeyValue.Fixed)
    }

  def apply(newKeyValue: KeyValue.Function,
            oldKeyValue: KeyValue.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): KeyValue.Fixed =
    if (newKeyValue.time > oldKeyValue.time)
      FixedMerger(
        newer = newKeyValue,
        oldApplies = oldKeyValue.getOrFetchApplies
      )
    else
      oldKeyValue
}

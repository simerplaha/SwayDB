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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{Memory, SwayFunction, SwayFunctionOutput, Value}
import swaydb.core.function.FunctionStore
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object FunctionMerger {

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.Put)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                       functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          oldKeyValue.copyWithDeadlineAndTime(Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Put(oldKeyValue.key, value, deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.getOrFetchFunction flatMap {
        function =>
          functionStore.get(function) match {
            case Some(functionId) =>
              functionId match {
                case SwayFunction.Value(f) =>
                  oldKeyValue
                    .getOrFetchValue
                    .flatMap(value => IO(f(value)))
                    .map(applyOutput)

                case SwayFunction.ValueDeadline(f) =>
                  oldKeyValue
                    .getOrFetchValue
                    .flatMap(value => IO(f(value, oldKeyValue.deadline)))
                    .map(applyOutput)

                case function: SwayFunction.RequiresKey =>
                  function match {
                    case SwayFunction.Key(f) =>
                      IO(applyOutput(f(oldKeyValue.key)))

                    case SwayFunction.KeyValue(f) =>
                      oldKeyValue
                        .getOrFetchValue
                        .flatMap(oldValue => IO(f(oldKeyValue.key, oldValue)))
                        .map(applyOutput)

                    case SwayFunction.KeyDeadline(f) =>
                      IO(applyOutput(f(oldKeyValue.key, oldKeyValue.deadline)))

                    case SwayFunction.KeyValueDeadline(f) =>
                      oldKeyValue
                        .getOrFetchValue
                        .flatMap(oldValue => IO(f(oldKeyValue.key, oldValue, oldKeyValue.deadline)))
                        .map(applyOutput)
                  }
              }

            case None =>
              IO.Left[swaydb.Error.Segment, ReadOnly.Fixed](swaydb.Error.FunctionNotFound(function))
          }
      }

    else
      IO.Right(oldKeyValue)
  }

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.Update)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          oldKeyValue.copyWithDeadlineAndTime(Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Update(oldKeyValue.key, value, deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    def toPendingApply(): IO[swaydb.Error.Segment, Memory.PendingApply] =
      for {
        oldValue <- oldKeyValue.toFromValue()
        newValue <- newKeyValue.toFromValue()
      } yield {
        Memory.PendingApply(oldKeyValue.key, Slice(oldValue, newValue))
      }

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.getOrFetchFunction flatMap {
        function =>
          functionStore.get(function) match {
            case Some(functionId) =>
              functionId match {
                case SwayFunction.Value(f) =>
                  oldKeyValue
                    .getOrFetchValue
                    .flatMap(value => IO(f(value)))
                    .map(applyOutput)

                case SwayFunction.ValueDeadline(f) =>
                  //if deadline is not set, then the deadline of this key might have another update in lower levels.
                  //so stash update.
                  if (oldKeyValue.deadline.isEmpty)
                    toPendingApply()
                  else
                    oldKeyValue
                      .getOrFetchValue
                      .flatMap(value => IO(f(value, oldKeyValue.deadline)))
                      .map(applyOutput)

                case function: SwayFunction.RequiresKey =>
                  if (oldKeyValue.key.isEmpty)
                    toPendingApply()
                  else
                    function match {
                      case SwayFunction.Key(f) =>
                        IO(applyOutput(f(oldKeyValue.key)))

                      case SwayFunction.KeyValue(f) =>
                        oldKeyValue
                          .getOrFetchValue
                          .flatMap(oldValue => IO(f(oldKeyValue.key, oldValue)))
                          .map(applyOutput)

                      case SwayFunction.KeyDeadline(f) =>
                        if (oldKeyValue.deadline.isEmpty)
                          toPendingApply()
                        else
                          IO(applyOutput(f(oldKeyValue.key, oldKeyValue.deadline)))

                      case SwayFunction.KeyValueDeadline(f) =>
                        if (oldKeyValue.deadline.isEmpty)
                          toPendingApply()
                        else
                          oldKeyValue
                            .getOrFetchValue
                            .flatMap(oldValue => IO(f(oldKeyValue.key, oldValue, oldKeyValue.deadline)))
                            .map(applyOutput)
                    }
              }

            case None =>
              IO.Left[swaydb.Error.Segment, ReadOnly.Fixed](swaydb.Error.FunctionNotFound(function))
          }
      }
    else
      IO.Right(oldKeyValue)
  }

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.Remove)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                          functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] = {

    def applyOutput(output: SwayFunctionOutput) =
      output match {
        case SwayFunctionOutput.Nothing =>
          oldKeyValue.copyWithTime(newKeyValue.time)

        case SwayFunctionOutput.Remove =>
          Memory.Remove(oldKeyValue.key, None, newKeyValue.time)

        case SwayFunctionOutput.Expire(deadline) =>
          Memory.Remove(oldKeyValue.key, Some(deadline), newKeyValue.time)

        case SwayFunctionOutput.Update(value, deadline) =>
          Memory.Update(oldKeyValue.key, value, deadline.orElse(oldKeyValue.deadline), newKeyValue.time)
      }

    def toPendingApply() =
      newKeyValue.toFromValue() map {
        newValue =>
          Memory.PendingApply(oldKeyValue.key, Slice(oldKeyValue.toRemoveValue(), newValue))
      }

    if (newKeyValue.time > oldKeyValue.time)
      newKeyValue.getOrFetchFunction flatMap {
        function =>
          oldKeyValue.deadline match {
            case None =>
              IO.Right(oldKeyValue.copyWithTime(newKeyValue.time))

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
                      IO(applyOutput(f(oldKeyValue.key)))

                    case SwayFunction.KeyDeadline(f) =>
                      IO(applyOutput(f(oldKeyValue.key, oldKeyValue.deadline)))
                  }

                case None =>
                  IO.Left[swaydb.Error.Segment, ReadOnly.Fixed](swaydb.Error.FunctionNotFound(function))
              }
          }
      }

    else
      IO.Right(oldKeyValue)
  }

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.Function)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                            functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      for {
        oldValue <- oldKeyValue.toFromValue()
        newValue <- newKeyValue.toFromValue()
      } yield {
        Memory.PendingApply(newKeyValue.key, Slice(oldValue, newValue))
      }
    else
      IO.Right(oldKeyValue)

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.Fixed)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                         functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    oldKeyValue match {
      case oldKeyValue: ReadOnly.Put =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: ReadOnly.Remove =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: ReadOnly.Update =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: ReadOnly.Function =>
        FunctionMerger(newKeyValue, oldKeyValue)

      case oldKeyValue: ReadOnly.PendingApply =>
        FunctionMerger(newKeyValue, oldKeyValue)
    }

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: Value.Apply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    oldKeyValue match {
      case oldKeyValue: Value.Remove =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): ReadOnly.Fixed)

      case oldKeyValue: Value.Update =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): ReadOnly.Fixed)

      case oldKeyValue: Value.Function =>
        FunctionMerger(newKeyValue, oldKeyValue.toMemory(newKeyValue.key): ReadOnly.Fixed)
    }

  def apply(newKeyValue: ReadOnly.Function,
            oldKeyValue: ReadOnly.PendingApply)(implicit timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore): IO[swaydb.Error.Segment, ReadOnly.Fixed] =
    if (newKeyValue.time > oldKeyValue.time)
      oldKeyValue
        .getOrFetchApplies
        .flatMap {
          oldApplies =>
            FixedMerger(newKeyValue, oldApplies)
        }
    else
      IO.Right(oldKeyValue)
}

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

package swaydb.core.level.seek

import swaydb.Error.Level.ExceptionHandler
import swaydb.IO
import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FunctionMerger, PendingApplyMerger, RemoveMerger, UpdateMerger}
import swaydb.core.segment.ReadState
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[core] object Get {

  def seek(key: Slice[Byte],
           readState: ReadState,
           currentGetter: CurrentGetter,
           nextGetter: NextGetter)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] =
    Get(key = key, readState = readState)(
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      currentGetter = currentGetter,
      nextGetter = nextGetter,
      functionStore = functionStore
    )

  def apply(key: Slice[Byte],
            readState: ReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  currentGetter: CurrentGetter,
                                  nextGetter: NextGetter,
                                  functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] = {

    @tailrec
    def resolve(current: KeyValue): IO.Defer[swaydb.Error.Level, Option[KeyValue.Put]] =
      current match {
        case current: KeyValue.Put =>
          if (current.hasTimeLeft())
            IO.Defer(Some(current))
          else
            IO.Defer.none

        case current: KeyValue.Remove =>
          if (current.hasTimeLeft())
            nextGetter
              .get(key, readState)
              .map {
                nextOption =>
                  nextOption
                    .flatMap {
                      next =>
                        if (next.hasTimeLeft())
                          RemoveMerger(current, next) match {
                            case put: KeyValue.Put if put.hasTimeLeft() =>
                              Some(put)

                            case _: KeyValue.Fixed =>
                              None
                          }
                        else
                          None
                    }
              }
          else
            IO.Defer.none

        case current: KeyValue.Update =>
          if (current.hasTimeLeft())
            nextGetter
              .get(key, readState)
              .map {
                nextOption =>
                  nextOption
                    .flatMap {
                      next =>
                        if (next.hasTimeLeft())
                          UpdateMerger(current, next) match {
                            case put: KeyValue.Put if put.hasTimeLeft() =>
                              Some(put)

                            case _: KeyValue.Fixed =>
                              None
                          }
                        else
                          None
                    }
              }
          else
            IO.Defer.none

        case current: KeyValue.Range =>
          val currentValue =
            try
              if (keyOrder.equiv(current.key, key))
                current.fetchFromOrElseRangeValueUnsafe
              else
                current.fetchRangeValueUnsafe
            catch {
              case throwable: Throwable =>
                return IO.Left(IO.ExceptionHandler.toError(throwable)) recoverTo Get(key, readState)
            }

          if (Value.hasTimeLeft(currentValue))
            resolve(currentValue.toMemory(key))
          else
            IO.Defer.none

        case current: KeyValue.Function =>
          nextGetter
            .get(key, readState)
            .flatMap {
              case Some(next) =>
                if (next.hasTimeLeft())
                  try
                    FunctionMerger(current, next) match {
                      case put: KeyValue.Put if put.hasTimeLeft() =>
                        IO.Defer(Some(put))

                      case _: KeyValue.Fixed =>
                        IO.Defer.none
                    }
                  catch {
                    case throwable: Throwable =>
                      IO.Left(IO.ExceptionHandler.toError(throwable)) recoverTo Get(key, readState)
                  }
                else
                  IO.Defer.none

              case None =>
                IO.Defer.none
            }

        case current: KeyValue.PendingApply =>
          nextGetter
            .get(key, readState)
            .flatMap {
              case Some(next) =>
                if (next.hasTimeLeft())
                  try
                    PendingApplyMerger(current, next) match {
                      case put: KeyValue.Put if put.hasTimeLeft() =>
                        IO.Defer(Some(put))

                      case _: KeyValue.Fixed =>
                        IO.Defer.none
                    }
                  catch {
                    case throwable: Throwable =>
                      IO.Left(IO.ExceptionHandler.toError(throwable)) recoverTo Get(key, readState)
                  }
                else
                  IO.Defer.none

              case None =>
                IO.Defer.none
            }
      }

    val current =
      try
        currentGetter.get(key, readState)
      catch {
        case throwable: Throwable =>
          return IO.Left(IO.ExceptionHandler.toError(throwable)) recoverTo Get(key, readState)
      }

    current match {
      case Some(current) =>
        resolve(current)

      case None =>
        nextGetter.get(key, readState)
    }
  }
}

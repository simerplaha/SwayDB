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

import swaydb.Error.Level.ErrorHandler
import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FunctionMerger, PendingApplyMerger, RemoveMerger, UpdateMerger}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[core] object Get {

  def seek(key: Slice[Byte],
           currentGetter: CurrentGetter,
           nextGetter: NextGetter)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] =
    Get(key = key)(
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      currentGetter = currentGetter,
      nextGetter = nextGetter,
      functionStore = functionStore
    )

  def apply(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]],
                              timeOrder: TimeOrder[Slice[Byte]],
                              currentGetter: CurrentGetter,
                              nextGetter: NextGetter,
                              functionStore: FunctionStore): IO.Defer[swaydb.Error.Level, Option[KeyValue.ReadOnly.Put]] = {

    import keyOrder._

    @tailrec
    def returnSegmentResponse(current: KeyValue.ReadOnly.SegmentResponse): IO.Defer[swaydb.Error.Level, Option[ReadOnly.Put]] =
      current match {
        case current: KeyValue.ReadOnly.Remove =>
          if (current.hasTimeLeft())
            nextGetter.get(key) map {
              nextOption =>
                nextOption flatMap {
                  next =>
                    if (next.hasTimeLeft())
                      RemoveMerger(current, next) match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          Some(put)

                        case _: ReadOnly.Fixed =>
                          None
                      }
                    else
                      None
                }
            }
          else
            IO.Defer.none

        case current: KeyValue.ReadOnly.Put =>
          if (current.hasTimeLeft())
            IO.Defer(Some(current))
          else
            IO.Defer.none

        case current: KeyValue.ReadOnly.Update =>
          if (current.hasTimeLeft())
            nextGetter.get(key) map {
              nextOption =>
                nextOption flatMap {
                  next =>
                    if (next.hasTimeLeft())
                      UpdateMerger(current, next) match {
                        case put: ReadOnly.Put if put.hasTimeLeft() =>
                          Some(put)

                        case _: ReadOnly.Fixed =>
                          None
                      }
                    else
                      None
                }
            }
          else
            IO.Defer.none

        case current: KeyValue.ReadOnly.Range =>
          (if (current.key equiv key) current.fetchFromOrElseRangeValue else current.fetchRangeValue) match {
            case IO.Success(currentValue) =>
              if (Value.hasTimeLeft(currentValue))
                returnSegmentResponse(currentValue.toMemory(key))
              else
                IO.Defer.none

            case failure @ IO.Failure(_) =>
              failure recoverTo Get(key)
          }

        case current: KeyValue.ReadOnly.Function =>
          nextGetter.get(key) flatMap {
            nextOption =>
              nextOption map {
                next =>
                  if (next.hasTimeLeft())
                    FunctionMerger(current, next) match {
                      case IO.Success(put: ReadOnly.Put) if put.hasTimeLeft() =>
                        IO.Defer(Some(put))

                      case IO.Success(_: ReadOnly.Fixed) =>
                        IO.Defer.none

                      case failure @ IO.Failure(_) =>
                        failure recoverTo Get(key)
                    }
                  else
                    IO.Defer.none
              } getOrElse {
                IO.Defer.none
              }
          }

        case current: KeyValue.ReadOnly.PendingApply =>
          nextGetter.get(key) flatMap {
            nextOption =>
              nextOption map {
                next =>
                  if (next.hasTimeLeft())
                    PendingApplyMerger(current, next) match {
                      case IO.Success(put: ReadOnly.Put) if put.hasTimeLeft() =>
                        IO.Defer(Some(put))

                      case IO.Success(_: ReadOnly.Fixed) =>
                        IO.Defer.none

                      case failure @ IO.Failure(_) =>
                        failure recoverTo Get(key)
                    }
                  else
                    IO.Defer.none
              } getOrElse {
                IO.Defer.none
              }
          }
      }

    currentGetter.get(key) match {
      case IO.Success(Some(current)) =>
        returnSegmentResponse(current)

      case IO.Success(None) =>
        nextGetter.get(key)

      case failure @ IO.Failure(_) =>
        failure recoverTo Get(key)
    }
  }
}

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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.seek

import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FunctionMerger, PendingApplyMerger, RemoveMerger, UpdateMerger}
import swaydb.core.segment.ThreadReadState
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.annotation.tailrec

private[core] object Get {

  def seek(key: Slice[Byte],
           readState: ThreadReadState,
           currentGetter: CurrentGetter,
           nextGetter: NextGetter)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                   timeOrder: TimeOrder[Slice[Byte]],
                                   functionStore: FunctionStore): KeyValue.PutOption =
    Get(key = key, readState = readState)(
      keyOrder = keyOrder,
      timeOrder = timeOrder,
      currentGetter = currentGetter,
      nextGetter = nextGetter,
      functionStore = functionStore
    )

  def apply(key: Slice[Byte],
            readState: ThreadReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                        timeOrder: TimeOrder[Slice[Byte]],
                                        currentGetter: CurrentGetter,
                                        nextGetter: NextGetter,
                                        functionStore: FunctionStore): KeyValue.PutOption = {

    @tailrec
    def resolve(current: KeyValue): KeyValue.PutOption =
      current match {
        case current: KeyValue.Put =>
          if (current.hasTimeLeft())
            current
          else
            KeyValue.Put.Null

        case current: KeyValue.Remove =>
          if (current.hasTimeLeft())
            nextGetter
              .get(key, readState)
              .flatMap {
                next =>
                  if (next.hasTimeLeft())
                    RemoveMerger(current, next) match {
                      case put: KeyValue.Put if put.hasTimeLeft() =>
                        put

                      case _: KeyValue.Fixed =>
                        KeyValue.Put.Null
                    }
                  else
                    KeyValue.Put.Null
              }
          else
            KeyValue.Put.Null

        case current: KeyValue.Update =>
          if (current.hasTimeLeft())
            nextGetter
              .get(key, readState)
              .flatMap {
                next =>
                  if (next.hasTimeLeft())
                    UpdateMerger(current, next) match {
                      case put: KeyValue.Put if put.hasTimeLeft() =>
                        put

                      case _: KeyValue.Fixed =>
                        KeyValue.Put.Null
                    }
                  else
                    KeyValue.Put.Null
              }
          else
            KeyValue.Put.Null

        case current: KeyValue.Range =>
          val currentValue =
            if (keyOrder.equiv(current.key, key))
              current.fetchFromOrElseRangeValueUnsafe
            else
              current.fetchRangeValueUnsafe

          if (Value.hasTimeLeft(currentValue))
            resolve(currentValue.toMemory(key))
          else
            KeyValue.Put.Null

        case current: KeyValue.Function =>
          nextGetter
            .get(key, readState)
            .flatMap {
              next =>
                if (next.hasTimeLeft())
                  FunctionMerger(current, next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _: KeyValue.Fixed =>
                      KeyValue.Put.Null
                  }
                else
                  KeyValue.Put.Null
            }

        case current: KeyValue.PendingApply =>
          nextGetter
            .get(key, readState)
            .flatMap {
              next =>
                if (next.hasTimeLeft())
                  PendingApplyMerger(current, next) match {
                    case put: KeyValue.Put if put.hasTimeLeft() =>
                      put

                    case _: KeyValue.Fixed =>
                      KeyValue.Put.Null
                  }
                else
                  KeyValue.Put.Null
            }
      }

    currentGetter.get(key, readState) match {
      case current: KeyValue =>
        resolve(current)

      case _: KeyValue.Null =>
        nextGetter.get(key, readState)
    }
  }
}

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

package swaydb.core.level.seek

import swaydb.core.data.{KeyValue, Value}
import swaydb.core.function.FunctionStore
import swaydb.core.merge.{FunctionMerger, PendingApplyMerger, RemoveMerger, UpdateMerger}
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice

import scala.annotation.tailrec

private[core] object Get {

  @inline def seek(key: Slice[Byte],
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

  @inline def apply(key: Slice[Byte],
                    readState: ThreadReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                currentGetter: CurrentGetter,
                                                nextGetter: NextGetter,
                                                functionStore: FunctionStore): KeyValue.PutOption =
    currentGetter.get(key, readState) match {
      case current: KeyValue =>
        resolve(
          current = current,
          key = key,
          readState = readState
        )

      case _: KeyValue.Null =>
        nextGetter.get(
          key = key,
          readState = readState
        )
    }

  @tailrec
  private def resolve(current: KeyValue,
                      key: Slice[Byte],
                      readState: ThreadReadState)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                  timeOrder: TimeOrder[Slice[Byte]],
                                                  currentGetter: CurrentGetter,
                                                  nextGetter: NextGetter,
                                                  functionStore: FunctionStore): KeyValue.PutOption =
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
          resolve(current = currentValue.toMemory(key), key = key, readState = readState)
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
}

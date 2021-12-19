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

package swaydb.core.segment

import swaydb.core.segment.Segment.getNearestPutDeadline
import swaydb.core.segment.data.{KeyValue, Value}
import swaydb.core.util.MinMax
import swaydb.slice.Slice
import swaydb.utils.FiniteDurations

import scala.concurrent.duration.Deadline

private object DeadlineAndFunctionId {
  val empty: DeadlineAndFunctionId =
    apply(None, None)

  @inline def apply(deadline: Option[Deadline],
                    minMaxFunctionId: Option[MinMax[Slice[Byte]]]): DeadlineAndFunctionId =
    new DeadlineAndFunctionId(
      nearestDeadline = deadline,
      minMaxFunctionId = minMaxFunctionId
    )

  @inline def apply(keyValues: Iterable[KeyValue]): DeadlineAndFunctionId =
    keyValues.foldLeft(DeadlineAndFunctionId.empty) {
      case (minMax, keyValue) =>
        apply(
          deadline = minMax.nearestDeadline,
          minMaxFunctionId = minMax.minMaxFunctionId,
          next = keyValue
        )
    }

  def apply(deadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            next: KeyValue): DeadlineAndFunctionId =
    next match {
      case put: KeyValue.Put =>
        DeadlineAndFunctionId(
          deadline = FiniteDurations.getNearestDeadline(deadline, put.deadline),
          minMaxFunctionId = minMaxFunctionId
        )

      case _: KeyValue.Remove =>
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = minMaxFunctionId
        )

      case _: KeyValue.Update =>
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = minMaxFunctionId
        )

      case pendingApply: KeyValue.PendingApply =>
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = MinMax.minMaxFunction(pendingApply.getOrFetchApplies, minMaxFunctionId)
        )

      case function: KeyValue.Function =>
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = Some(MinMax.minMaxFunction(function, minMaxFunctionId))
        )

      case range: KeyValue.Range =>
        range.fetchFromAndRangeValueUnsafe match {
          case (fromValue: Value.FromValue, rangeValue) =>
            DeadlineAndFunctionId(
              deadline = getNearestPutDeadline(getNearestPutDeadline(deadline, fromValue), rangeValue),
              minMaxFunctionId = MinMax.minMaxFunction(fromValue, rangeValue, minMaxFunctionId)
            )

          case (Value.FromValue.Null, rangeValue) =>
            DeadlineAndFunctionId(
              deadline = getNearestPutDeadline(deadline, rangeValue),
              minMaxFunctionId = MinMax.minMaxFunction(Value.FromValue.Null, rangeValue, minMaxFunctionId)
            )
        }
    }
}

@inline private class DeadlineAndFunctionId(val nearestDeadline: Option[Deadline],
                                            val minMaxFunctionId: Option[MinMax[Slice[Byte]]])

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

package swaydb.core.segment

import swaydb.core.data.{KeyValue, Value}
import swaydb.core.segment.Segment.getNearestPutDeadline
import swaydb.core.util.MinMax
import swaydb.data.slice.Slice
import swaydb.utils.FiniteDurations

import scala.concurrent.duration.Deadline

protected object DeadlineAndFunctionId {
  val empty: DeadlineAndFunctionId =
    apply(None, None)

  def apply(deadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]]): DeadlineAndFunctionId =
    new DeadlineAndFunctionId(
      nearestDeadline = deadline,
      minMaxFunctionId = minMaxFunctionId
    )

  def apply(keyValues: Iterable[KeyValue]): DeadlineAndFunctionId =
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
      case readOnly: KeyValue.Put =>
        DeadlineAndFunctionId(
          deadline = FiniteDurations.getNearestDeadline(deadline, readOnly.deadline),
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

      case readOnly: KeyValue.PendingApply =>
        val applies = readOnly.getOrFetchApplies
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = MinMax.minMaxFunction(applies, minMaxFunctionId)
        )

      case readOnly: KeyValue.Function =>
        DeadlineAndFunctionId(
          deadline = deadline,
          minMaxFunctionId = Some(MinMax.minMaxFunction(readOnly, minMaxFunctionId))
        )

      case range: KeyValue.Range =>
        range.fetchFromAndRangeValueUnsafe match {
          case (fromValue: Value.FromValue, rangeValue) =>
            val fromValueDeadline = getNearestPutDeadline(deadline, fromValue)
            DeadlineAndFunctionId(
              deadline = getNearestPutDeadline(fromValueDeadline, rangeValue),
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

protected class DeadlineAndFunctionId(val nearestDeadline: Option[Deadline],
                                      val minMaxFunctionId: Option[MinMax[Slice[Byte]]])

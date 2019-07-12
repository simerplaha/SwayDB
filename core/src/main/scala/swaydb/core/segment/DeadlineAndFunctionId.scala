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

package swaydb.core.segment

import swaydb.core.data.KeyValue
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.Segment.getNearestDeadline
import swaydb.core.util.{FiniteDurationUtil, MinMax}
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[core] object DeadlineAndFunctionId {
  val empty: DeadlineAndFunctionId =
    apply(None, None)

  def apply(deadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]]): DeadlineAndFunctionId =
    new DeadlineAndFunctionId(
      nearestDeadline = deadline,
      minMaxFunctionId = minMaxFunctionId
    )

  def apply(keyValues: Iterable[KeyValue.ReadOnly])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                    keyValueLimiter: KeyValueLimiter): IO[DeadlineAndFunctionId] =
    keyValues.foldLeftIO(DeadlineAndFunctionId.empty) {
      case (minMax, keyValue) =>
        apply(
          deadline = minMax.nearestDeadline,
          minMaxFunctionId = minMax.minMaxFunctionId,
          next = keyValue
        )
    }

  def apply(deadline: Option[Deadline],
            minMaxFunctionId: Option[MinMax[Slice[Byte]]],
            next: KeyValue.ReadOnly)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                     keyValueLimiter: KeyValueLimiter): IO[DeadlineAndFunctionId] =
    next match {
      case readOnly: KeyValue.ReadOnly.Put =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.Remove =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.Update =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.PendingApply =>
        readOnly.getOrFetchApplies map {
          applies =>
            DeadlineAndFunctionId(
              deadline = FiniteDurationUtil.getNearestDeadline(deadline, readOnly.deadline),
              minMaxFunctionId = MinMax.minMaxFunction(applies, minMaxFunctionId)
            )
        }

      case readOnly: KeyValue.ReadOnly.Function =>
        MinMax.minMaxFunction(readOnly, minMaxFunctionId) map {
          function =>
            DeadlineAndFunctionId(
              deadline = deadline,
              minMaxFunctionId = Some(function)
            )
        }

      case range: KeyValue.ReadOnly.Range =>
        range.fetchFromAndRangeValue map {
          case (someFromValue @ Some(fromValue), rangeValue) =>
            val fromValueDeadline = getNearestDeadline(deadline, fromValue)
            DeadlineAndFunctionId(
              deadline = getNearestDeadline(fromValueDeadline, rangeValue),
              minMaxFunctionId = MinMax.minMaxFunction(someFromValue, rangeValue, minMaxFunctionId)
            )

          case (None, rangeValue) =>
            DeadlineAndFunctionId(
              deadline = getNearestDeadline(deadline, rangeValue),
              minMaxFunctionId = MinMax.minMaxFunction(None, rangeValue, minMaxFunctionId)
            )
        }

      case group: KeyValue.ReadOnly.Group =>
        val nextDeadline = FiniteDurationUtil.getNearestDeadline(deadline, group.deadline)

        group
          .segment
          .getAll()
          .flatMap {
            keyValues =>
              keyValues.foldLeftIO(DeadlineAndFunctionId(nextDeadline, minMaxFunctionId)) {
                case (minMax, keyValue) =>
                  apply(
                    deadline = minMax.nearestDeadline,
                    minMaxFunctionId = minMax.minMaxFunctionId,
                    next = keyValue
                  )
              }
          }
    }
}

private[core] class DeadlineAndFunctionId(val nearestDeadline: Option[Deadline],
                                          val minMaxFunctionId: Option[MinMax[Slice[Byte]]])

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

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IO._
import swaydb.core.actor.MemorySweeper
import swaydb.core.data.KeyValue
import swaydb.core.segment.Segment.getNearestDeadline
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.util.{FiniteDurations, MinMax}
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
                                                    memorySweeper: Option[MemorySweeper.KeyValue],
                                                    segmentIO: SegmentIO): IO[swaydb.Error.Segment, DeadlineAndFunctionId] =
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
                                     memorySweeper: Option[MemorySweeper.KeyValue],
                                     segmentIO: SegmentIO): IO[swaydb.Error.Segment, DeadlineAndFunctionId] =
    next match {
      case readOnly: KeyValue.ReadOnly.Put =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurations.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.Remove =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurations.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.Update =>
        IO {
          DeadlineAndFunctionId(
            deadline = FiniteDurations.getNearestDeadline(deadline, readOnly.deadline),
            minMaxFunctionId = minMaxFunctionId
          )
        }

      case readOnly: KeyValue.ReadOnly.PendingApply =>
        readOnly.getOrFetchApplies map {
          applies =>
            DeadlineAndFunctionId(
              deadline = FiniteDurations.getNearestDeadline(deadline, readOnly.deadline),
              minMaxFunctionId = MinMax.minMaxFunction(applies, minMaxFunctionId)
            )
        }

      case readOnly: KeyValue.ReadOnly.Function =>
        MinMax.minMaxFunction(readOnly, minMaxFunctionId) map {
          minMaxFunctionId =>
            DeadlineAndFunctionId(
              deadline = deadline,
              minMaxFunctionId = Some(minMaxFunctionId)
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
    }
}

private[core] class DeadlineAndFunctionId(val nearestDeadline: Option[Deadline],
                                          val minMaxFunctionId: Option[MinMax[Slice[Byte]]])

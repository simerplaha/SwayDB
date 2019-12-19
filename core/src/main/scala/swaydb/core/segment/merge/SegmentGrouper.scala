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

package swaydb.core.segment.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Memory, Persistent, Value, _}

import scala.collection.mutable.ListBuffer

/**
 * SegmentGroups will always group key-values with Groups at the head of key-value List. Groups cannot be randomly
 * added in the middle.
 */
private[merge] object SegmentGrouper extends LazyLogging {

  def add[T[_]](keyValue: KeyValue,
                builder: MergeStats[T],
                isLastLevel: Boolean): Unit =
    keyValue match {
      case fixed: KeyValue.Fixed =>
        fixed match {
          case put: Memory.Put =>
            if (!isLastLevel || put.hasTimeLeft())
              builder add put

          case put: Persistent.Put =>
            if (!isLastLevel || put.hasTimeLeft())
              builder add put.toMemory()

          case remove: Memory.Remove =>
            if (!isLastLevel)
              builder add remove

          case remove: Persistent.Remove =>
            if (!isLastLevel)
              builder add remove.toMemory()

          case update: Memory.Update =>
            if (!isLastLevel)
              builder add update

          case update: Persistent.Update =>
            if (!isLastLevel)
              builder add update.toMemory()

          case function: Memory.Function =>
            if (!isLastLevel)
              builder add function

          case function: Persistent.Function =>
            if (!isLastLevel)
              builder add function.toMemory()

          case pending: Memory.PendingApply =>
            if (!isLastLevel)
              builder add pending

          case pendingApply: Persistent.PendingApply =>
            if (!isLastLevel)
              builder add pendingApply.toMemory()
        }

      case range: KeyValue.Range =>
        if (isLastLevel) {
          range.fetchFromValueUnsafe foreach {
            case put @ Value.Put(fromValue, deadline, time) =>
              if (put.hasTimeLeft())
                builder add
                  Memory.Put(
                    key = range.fromKey,
                    value = fromValue,
                    deadline = deadline,
                    time = time
                  )

            case _: Value.Remove | _: Value.Update | _: Value.Function | _: Value.PendingApply =>
              ()
          }
        } else {
          val (fromValue, rangeValue) = range.fetchFromAndRangeValueUnsafe
          builder add
            Memory.Range(
              fromKey = range.fromKey,
              toKey = range.toKey,
              fromValue = fromValue,
              rangeValue = rangeValue
            )
        }
    }
}

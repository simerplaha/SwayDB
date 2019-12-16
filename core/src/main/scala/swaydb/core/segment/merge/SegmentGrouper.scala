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

  def addKeyValue(keyValue: KeyValue,
                  result: MergeBuilder,
                  isLastLevel: Boolean): Unit =
    keyValue match {
      case fixed: KeyValue.Fixed =>
        fixed match {
          case put: Memory.Put =>
            if (!isLastLevel || put.hasTimeLeft())
              result addOne put

          case put: Persistent.Put =>
            if (!isLastLevel || put.hasTimeLeft())
              result addOne put.toMemory()

          case remove: Memory.Remove =>
            if (!isLastLevel)
              result addOne remove

          case remove: Persistent.Remove =>
            if (!isLastLevel)
              result addOne remove.toMemory()

          case update: Memory.Update =>
            if (!isLastLevel)
              result addOne update

          case update: Persistent.Update =>
            if (!isLastLevel)
              result addOne update.toMemory()

          case function: Memory.Function =>
            if (!isLastLevel)
              result addOne function

          case function: Persistent.Function =>
            if (!isLastLevel)
              result addOne function.toMemory()

          case pending: Memory.PendingApply =>
            if (!isLastLevel)
              result addOne pending

          case pendingApply: Persistent.PendingApply =>
            if (!isLastLevel)
              result addOne pendingApply.toMemory()
        }

      case range: KeyValue.Range =>
        if (isLastLevel) {
          range.fetchFromValueUnsafe foreach {
            case put @ Value.Put(fromValue, deadline, time) =>
              if (put.hasTimeLeft())
                result addOne
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
          result addOne
            Memory.Range(
              fromKey = range.fromKey,
              toKey = range.toKey,
              fromValue = fromValue,
              rangeValue = rangeValue
            )
        }
    }
}

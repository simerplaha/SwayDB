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

package swaydb.core.segment.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Memory, Persistent, Value, _}

/**
 * SegmentGroups will always group key-values with Groups at the head of key-value List. Groups cannot be randomly
 * added in the middle.
 */
private[core] object SegmentGrouper extends LazyLogging {

  def add[T[_]](keyValue: KeyValue,
                builder: MergeStats[Memory, T],
                isLastLevel: Boolean): Unit = {
    if (isLastLevel) {
      val keyValueToMerge = addLastLevel(keyValue)
      if (keyValueToMerge != null)
        builder add keyValueToMerge
    } else {
      builder add keyValue.toMemory
    }
  }

  def addLastLevel(keyValue: KeyValue): Memory =
    keyValue match {
      case fixed: KeyValue.Fixed =>
        fixed match {
          case put: Memory.Put =>
            if (put.hasTimeLeft())
              put
            else
              null

          case put: Persistent.Put =>
            if (put.hasTimeLeft())
              put.toMemory()
            else
              null

          case _: Memory.Fixed | _: Persistent.Fixed =>
            null
        }

      case range: KeyValue.Range =>
        val fromValue = range.fetchFromValueUnsafe
        if (fromValue.isSomeS)
          fromValue.getS match {
            case put @ Value.Put(fromValue, deadline, time) =>
              if (put.hasTimeLeft())
                Memory.Put(
                  key = range.fromKey,
                  value = fromValue,
                  deadline = deadline,
                  time = time
                )
              else
                null

            case _: Value.Remove | _: Value.Update | _: Value.Function | _: Value.PendingApply =>
              null
          }
        else
          null
    }
}

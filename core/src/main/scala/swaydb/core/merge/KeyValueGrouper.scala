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

package swaydb.core.merge

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.{Memory, Persistent, Value, _}
import swaydb.core.merge.stats.MergeStats

/**
 * SegmentGroups will always group key-values with Groups at the head of key-value List. Groups cannot be randomly
 * added in the middle.
 */
private[core] object KeyValueGrouper extends LazyLogging {

  def add[T[_]](keyValue: KeyValue,
                builder: MergeStats[Memory, T],
                isLastLevel: Boolean): Unit = {
    if (isLastLevel) {
      val keyValueToMergeOrNull = toLastLevelOrNull(keyValue)
      if (keyValueToMergeOrNull != null)
        builder add keyValueToMergeOrNull
    } else {
      builder add keyValue.toMemory()
    }
  }

  def toLastLevelOrNull(keyValue: KeyValue): Memory =
    keyValue match {
      case put: KeyValue.Put =>
        if (put.hasTimeLeft())
          put.toMemory()
        else
          null

      case _: KeyValue.Fixed =>
        null

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

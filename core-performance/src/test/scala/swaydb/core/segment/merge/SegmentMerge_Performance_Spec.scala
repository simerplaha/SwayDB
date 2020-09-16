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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.merge

import swaydb.core.TestData._
import swaydb.core.data.{Memory, Time}
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice


import scala.collection.mutable.ListBuffer

class SegmentMerge_Performance_Spec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val testTimer: TestTimer = TestTimer.Empty
  val keyValueCount = 100

  "performance" in {
    val keys = (1 to 1000000).map(Slice.writeInt[Byte])
    val keyValues = Slice.of[Memory.Put](keys.size)

    Benchmark("Creating key-values") {
      keys.foldLeft(Option.empty[Memory.Put]) {
        case (previous, key) =>
          val put =
            Memory.Put(
              key = key,
              value = key,
              deadline = None,
              time = Time.empty,
            )

          keyValues add put

          Some(put)
      }
    }

    Benchmark(s"SegmentMerger performance") {
      SegmentMerger.merge(
        newKeyValues = keyValues,
        oldKeyValues = keyValues,
        stats = MergeStats.persistent(ListBuffer.newBuilder),
        isLastLevel = false
      )
    }
  }
}

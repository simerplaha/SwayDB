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

import swaydb.core.segment.data.{Memory, Time}
import swaydb.Benchmark
import swaydb.core.{TestBase, TestTimer}
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.Slice
import swaydb.testkit.RunThis._

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

    //run multiple times because first is warmup
    runThis(4.times, log = true) {
      Benchmark(s"SegmentMerger performance") {
        //        KeyValueMerger.merge(
        //          newKeyValues = keyValues,
        //          oldKeyValues = keyValues,
        //          stats = MergeStats.persistent(Aggregator.listBuffer),
        //          isLastLevel = false
        //        )
        ???
      }
    }
  }
}

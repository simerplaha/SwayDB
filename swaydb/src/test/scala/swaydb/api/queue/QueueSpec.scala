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
 */

package swaydb.api.queue

import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.core.TestBase
import swaydb.core.util.Benchmark
import swaydb.serializers.Default._

import scala.collection.parallel.CollectionConverters._
import scala.jdk.CollectionConverters._

class QueueSpec extends TestBase {

  "it" in {
    val queue = swaydb.persistent.Queue[Int](randomDir).get
    val processedQueue = new ConcurrentLinkedQueue[Int]()

    val maxPushes = 1000000

    val pushRange = 0 to maxPushes
    //give pop extra to test that overflow is handled.
    val popRange = 0 to (maxPushes + 1000)

    def doAssert() = {
      //push jobs
      Benchmark(s"Pushing: $maxPushes") {
        pushRange foreach {
          int =>
            queue.push(int)
        }
      }

      //concurrently process the queue
      Benchmark(s"Popping: $maxPushes") {
        popRange.par foreach {
          _ =>
            queue.pop() foreach processedQueue.add
        }
      }

      //assert that all items in the queue are processed.
      processedQueue.size() shouldBe pushRange.size
      processedQueue.asScala.toList.distinct.size shouldBe pushRange.size
      processedQueue.clear()
    }

    doAssert()
    doAssert()
  }
}

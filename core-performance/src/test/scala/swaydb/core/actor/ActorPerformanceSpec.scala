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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.actor

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.ActorConfig.QueueOrder
import swaydb.core.TestExecutionContext
import swaydb.core.util.Benchmark
import swaydb.{Actor, ActorRef}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._

class ActorPerformanceSpec extends AnyWordSpec with Matchers {

  implicit val ec = TestExecutionContext.executionContext
  implicit val ordering = QueueOrder.FIFO

  "performance test" in {
    //0.251675378 seconds.
    //    val actor =
    //      Actor.timerLoopCache[Int](100000, _ => 1, 5.second) {
    //        (_: Int, self: ActorRef[Int, Unit]) =>
    //      }

    val actor =
      Actor.timerCache[Int]("", 100000, _ => 1, 5.second) {
        (_: Int, self: ActorRef[Int, Unit]) =>
      }.start()


    //0.111304334 seconds.
    //    val actor =
    //      Actor.cache[Int](100000, _ => 1) {
    //        (_: Int, self: ActorRef[Int, Unit]) =>
    //      }

    //0.186314412 seconds.
    //    val actor =
    //      Actor[Int] {
    //        (_: Int, self: ActorRef[Int, Unit]) =>
    //      }

    val queue = new ConcurrentLinkedQueue[Int]()

    Benchmark("") {
      (1 to 1000000).par foreach {
        i =>
          actor send i
        //          queue.add(i)
      }
    }
  }
}

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

import swaydb.Queue
import swaydb.core.TestBase
import swaydb.core.util.Benchmark
import swaydb.serializers.Default._
import org.scalatest.OptionValues._

import scala.collection.parallel.CollectionConverters._
import scala.jdk.CollectionConverters._
import scala.util.Random

class QueueSpec0 extends QueueSpec {

  override def newQueue(): Queue[Int] =
    swaydb.persistent.Queue[Int](randomDir).get
}

class QueueSpec3 extends QueueSpec {
  override def newQueue(): Queue[Int] =
    swaydb.memory.Queue[Int]().get
}

sealed trait QueueSpec extends TestBase {

  def newQueue(): Queue[Int]

  "it" should {
    "concurrently process" in {
      val queue: Queue[Int] = newQueue()
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
        Benchmark(s"Asserting") {
          processedQueue.size() shouldBe pushRange.size
          processedQueue.asScala.toList.distinct.size shouldBe pushRange.size
          processedQueue.clear()
        }
      }

      doAssert()
      doAssert()

      queue.close()
    }

    "concurrently process in batches" in {
      val queue = newQueue()
      val processedQueue = new ConcurrentLinkedQueue[Int]()

      val items = 1 to 100000

      items.map(queue.push)

      val jobs = items.grouped(items.size / 20).map(_.toList).toList
      jobs should have size 20

      jobs foreach {
        job =>
          Benchmark("Pop") {
            Random.shuffle(job).par foreach {
              _ =>
                queue.pop() foreach processedQueue.add
            }
          }

          Benchmark("Assert") {
            processedQueue.size() shouldBe job.size
            processedQueue.asScala.toList.distinct should contain allElementsOf job
          }

          processedQueue.clear()
      }
    }

    "continue on restart" when {
      val path = randomDir
      val queue = swaydb.persistent.Queue[Int](path).get

      (1 to 6).map(queue.push)

      queue.pop().value shouldBe 1
      queue.pop().value shouldBe 2
      queue.push(1)

      queue.close()

      val reopen = swaydb.persistent.Queue[Int](path).get
      reopen.pop().value shouldBe 3
      reopen.pop().value shouldBe 4
      reopen.close()

      val reopen2 = swaydb.persistent.Queue[Int](path).get
      reopen2.pop().value shouldBe 5
      reopen2.pop().value shouldBe 6
      reopen2.pop().value shouldBe 1
      reopen2.close()
    }
  }
}

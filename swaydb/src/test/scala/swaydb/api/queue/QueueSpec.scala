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

package swaydb.api.queue

import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.{Bag, Queue}
import swaydb.core.TestBase
import swaydb.core.util.Benchmark
import swaydb.serializers.Default._
import org.scalatest.OptionValues._

import scala.collection.parallel.CollectionConverters._
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.concurrent.duration._

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

  "push and pop" in {
    val queue: Queue[Int] = newQueue()

    queue.push(1)
    queue.push(2)

    queue.popOrNull() shouldBe 1
    queue.popOrNull() shouldBe 2
  }

  "push and pop in FIFO manner" in {
    val queue: Queue[Int] = newQueue()

    Benchmark("Push")((1 to 1000000).foreach(queue.push))
    Benchmark("Pop")((1 to 1000000).foreach(item => queue.popOrNull() shouldBe item))

    //all popped out
    queue.pop() shouldBe empty
  }

  "push, expire, pop & stream" in {
    val queue: Queue[Int] = newQueue()

    def assertStreamIsEmpty() = queue.stream.materialize[Bag.Less].toList shouldBe empty

    queue.push(elem = 1, expireAfter = 1.seconds)
    queue.push(2)
    queue.stream.materialize[Bag.Less].toList should contain inOrderOnly(1, 2)

    Thread.sleep(1000)

    queue.stream.materialize[Bag.Less].toList should contain only 2
    queue.popOrNull() shouldBe 2
    queue.pop() shouldBe empty
    assertStreamIsEmpty()

    queue.push(elem = 3, expireAfter = 1.seconds)
    queue.stream.materialize[Bag.Less].toList should contain only 3
    queue.popOrNull() shouldBe 3
    assertStreamIsEmpty()

    queue.push(elem = 4, expireAfter = 1.seconds)
    queue.push(elem = 5)
    queue.push(elem = 6)
    queue.stream.materialize[Bag.Less].toList should contain inOrderOnly(4, 5, 6)

    Thread.sleep(1000)
    queue.stream.materialize[Bag.Less].toList should contain inOrderOnly(5, 6)
    queue.popOrNull() shouldBe 5
    queue.popOrNull() shouldBe 6
    assertStreamIsEmpty()
  }

  "concurrently process" in {
    val queue: Queue[Int] = newQueue()
    val processedQueue = new ConcurrentLinkedQueue[Int]()

    val maxPushes = 1000000

    val pushRange = 0 to maxPushes
    //give pop extra to test that overflow is handled.
    val popRange = 0 to (maxPushes + 1000)

    def doAssert() = {
      //push jobs
      Benchmark(s"Push: $maxPushes") {
        pushRange foreach {
          int =>
            queue.push(int)
        }
      }

      //concurrently process the queue
      Benchmark(s"Pop: $maxPushes") {
        popRange.par foreach {
          _ =>
            queue.pop() foreach processedQueue.add
        }
      }

      //assert that all items in the queue are processed.
      Benchmark("Assert") {
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

  "continue on restart" in {
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

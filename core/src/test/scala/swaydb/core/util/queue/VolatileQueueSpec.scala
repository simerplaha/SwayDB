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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util.queue

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.core.TestData._
import swaydb.core.TestExecutionContext
import swaydb.data.RunThis._

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class VolatileQueueSpec extends AnyWordSpec with Matchers {

  implicit class QueueShouldBe[A >: Null](queue: VolatileQueue[A]) {
    def shouldBe(list: List[A]) = {
      //iterator
      queue.iterator.toList shouldBe list

      //size
      queue.size shouldBe list.size

      //head and last
      queue.headOrNull() shouldBe (if (list.isEmpty) null else list.head)
      queue.lastOrNull() shouldBe (if (list.isEmpty) null else list.last)
      queue.headOption() shouldBe list.headOption
      queue.secondLastOrNull() shouldBe (if (list.size <= 1) null else list.takeRight(2).head)
    }
  }

  "addHead" when {
    "empty" should {
      "return empty state" in {
        val queue = VolatileQueue[Integer]()

        queue shouldBe List.empty
      }

      "insert head element" in {
        val queue = VolatileQueue[Integer]()
        queue.addHead(10)

        queue shouldBe List(10)
      }
    }

    "nonEmpty" should {
      "insert head element" in {
        val queue = VolatileQueue[Integer](1)

        queue shouldBe List(1)

        queue.addHead(0)

        queue shouldBe List(0, 1)
      }

      "insert multiple head element" in {
        val queue = VolatileQueue[Integer]()

        queue.addHead(3)
        queue.addHead(2)
        queue.addHead(1)

        queue shouldBe List(1, 2, 3)
      }
    }
  }

  "addLast" when {
    "empty" should {
      "insert last element" in {
        val queue = VolatileQueue[Integer]()
        queue.addLast(10)

        queue shouldBe List(10)
      }
    }

    "nonEmpty" should {
      "insert head element" in {
        val queue = VolatileQueue[Integer](0)

        queue.addLast(1)

        queue shouldBe List(0, 1)
      }

      "insert multiple head element" in {
        val queue = VolatileQueue[Integer]()

        queue.addLast(1)
        queue.addLast(2)
        queue.addLast(3)

        queue shouldBe List(1, 2, 3)
      }
    }
  }

  "removeLast" when {
    "empty" should {
      "fail" in {
        val queue = VolatileQueue[Integer]()

        assertThrows[Exception](queue.removeLast(10))

        queue shouldBe List.empty
      }
    }

    "invalid last" should {
      "fail" in {
        val queue = VolatileQueue[Integer](0)

        //last value is 0, not 10
        assertThrows[Exception](queue.removeLast(10))

        queue shouldBe List(0)
      }
    }

    "non empty" should {
      "remove last" in {
        val queue = VolatileQueue[Integer]()
        queue.addLast(1)
        queue.addLast(2)
        queue.addLast(3)
        queue.addLast(4)

        queue.addHead(0)

        queue shouldBe List(0, 1, 2, 3, 4)

        queue.removeLast(4)
        queue shouldBe List(0, 1, 2, 3)

        queue.removeLast(3)
        queue shouldBe List(0, 1, 2)

        queue.removeLast(2)
        queue shouldBe List(0, 1)

        queue.removeLast(1)
        queue shouldBe List(0)

        queue.removeLast(0)
        queue shouldBe List.empty

        //add last
        queue.addLast(Int.MaxValue)
        queue shouldBe List(Int.MaxValue)

        queue.removeLast(Int.MaxValue)
        queue shouldBe List.empty

        //add head
        queue.addHead(Int.MaxValue)
        queue shouldBe List(Int.MaxValue)

        queue.removeLast(Int.MaxValue)
        queue shouldBe List.empty
      }
    }
  }

  "replaceLast" when {
    "empty" should {
      "fail" in {
        val queue = VolatileQueue[Integer]()

        assertThrows[Exception](queue.replaceLast(0, 10))

        queue shouldBe List.empty
      }
    }

    "invalid last" should {
      "fail" in {
        val queue = VolatileQueue[Integer](0)

        //last value is 0, not 10
        assertThrows[Exception](queue.replaceLast(1, 10))

        queue shouldBe List(0)
      }
    }

    "non empty" should {
      "replace last" in {
        val queue = VolatileQueue[Integer]()

        queue.addLast(1)
        queue.addLast(2)
        queue shouldBe List(1, 2)

        queue.replaceLast(2, 3)
        queue shouldBe List(1, 3)

        queue.removeLast(3)
        queue shouldBe List(1)

        queue.addHead(0)
        queue.removeLast(1)
        queue.addLast(1)

        queue.replaceLast(1, 2)
        queue shouldBe List(0, 2)
      }
    }
  }

  "replaceLastTwo" when {
    "empty" should {
      "fail" in {
        val queue = VolatileQueue[Integer]()

        assertThrows[Exception](queue.replaceLastTwo(0, 1, 10))

        queue shouldBe List.empty
      }
    }

    "size == 1" when {
      "last exists" should {
        "fail" in {
          val queue = VolatileQueue[Integer](1)

          assertThrows[Exception](queue.replaceLastTwo(0, 1, 10))

          queue shouldBe List(1)
        }
      }

      "second last exists" should {
        "fail" in {
          val queue = VolatileQueue[Integer](0)

          assertThrows[Exception](queue.replaceLastTwo(0, 1, 10))

          queue shouldBe List(0)
        }
      }
    }

    "invalid last and second last" should {
      "fail" in {
        val queue = VolatileQueue[Integer](2, 3)

        assertThrows[Exception](queue.replaceLastTwo(0, 1, 10))

        queue shouldBe List(2, 3)
      }
    }

    "non empty" should {
      "replace last two" in {
        val queue = VolatileQueue[Integer](2, 3)
        queue.replaceLastTwo(2, 3, 1)

        queue shouldBe List(1)
      }

      "remove and replace last two" in {
        val queue = VolatileQueue[Integer](2, 3)

        queue.addHead(1)
        queue shouldBe List(1, 2, 3)
        queue.replaceLastTwo(2, 3, 4)
        queue shouldBe List(1, 4)
      }
    }
  }

  "concurrent" in {
    implicit val ec = TestExecutionContext.executionContext

    val queue = VolatileQueue[Integer]()

    val add =
      Future {
        (1 to 100000).par foreach {
          i =>
            if (i % 1000 == 0) println(s"Write: $i")
            if (randomBoolean())
              queue.addHead(i)
            else
              queue.addLast(i)
        }
      }

    val remove =
      Future {
        (1 to 10000).par foreach {
          i =>
            if (i % 100 == 0) println(s"Remove: $i")

            //failures will occur here because of concurrency. Ignore for this.
            //the goal is to test that threads are not blocked.
            try
              queue.removeLast(queue.lastOrNull())
            catch {
              case _: Exception =>

            }
        }
      }

    val read =
      Future {
        (1 to 10000).par foreach {
          i =>
            if (i % 100 == 0) println(s"Read: $i")
            queue.iterator.foreach(_ => ())
        }
      }

    Future.sequence(Seq(add, remove, read)).await(1.minute)
  }

  "read" in {
    val queue = VolatileQueue[Integer]()

    (1 to 100000) foreach {
      i =>
        queue.addLast(i)
    }

    (1 to 10000).par foreach {
      i =>
        if (i % 100 == 0) println(s"Iteration: $i")

        queue.iterator.foldLeft(1) {
          case (expected, next) =>
            next shouldBe expected
            expected + 1
        }
    }
  }
}

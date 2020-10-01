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

package swaydb.core.util

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Bag.Less
import swaydb.IO.ApiIO
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestExecutionContext
import swaydb.data.RunThis._
import swaydb.{Bag, IO}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


class AtomicRanges_FutureSpec extends AtomicRangesSpec[Future]()(Bag.future(TestExecutionContext.executionContext)) {
  override def get[A](a: Future[A]): A = Await.result(a, 60.seconds)

  override def getException(a: => Future[_]): Throwable = {
    val future = try a finally {} //wait for the future to complete
    sleep(1.second)
    future.value.get.failed.get //get it's result
  }
}

class AtomicRanges_IOSpec extends AtomicRangesSpec[IO.ApiIO] {
  override def get[A](a: IO.ApiIO[A]): A = a.get

  override def getException(a: => ApiIO[_]): Throwable =
    a.left.get.exception
}

class AtomicRanges_LessSpec extends AtomicRangesSpec[Bag.Less] {
  override def get[A](a: Bag.Less[A]): A = a

  override def getException(a: => Less[_]): Throwable =
    IO(a).left.get
}

class AtomicRanges_TrySpec extends AtomicRangesSpec[Try] {
  override def get[A](a: Try[A]): A = a.get

  override def getException(a: => Try[_]): Throwable =
    a.failed.get
}

abstract class AtomicRangesSpec[BAG[_]](implicit bag: Bag[BAG]) extends AnyWordSpec with Matchers {

  def get[A](a: BAG[A]): A

  def getException(a: => BAG[_]): Throwable

  implicit class Get[A](a: BAG[A]) {
    def await = get(a)
  }

  implicit val ec = TestExecutionContext.executionContext

  "execute" when {
    "no overlaps" in {
      implicit val atomic = AtomicRanges[Int]()

      @volatile var executed = false

      atomic
        .execute(fromKey = 1, toKey = 10, toKeyInclusive = randomBoolean()) {
          executed = true
        }
        .await

      executed shouldBe true

      atomic.isEmpty shouldBe true
    }

    "overlaps" when {
      "exist is exclusive" in {
        implicit val ranges = AtomicRanges[Int]()

        @volatile var firstExecuted: Long = 0
        @volatile var secondExecuted: Long = 0

        //execute first
        val future =
          Future {
            ranges
              .execute(fromKey = 10, toKey = 20, toKeyInclusive = true) {
                sleep(1.second)
                firstExecuted = System.nanoTime()
                println("First executed")
              }
          }

        Future {
          ranges.execute(fromKey = 20, toKey = 30, toKeyInclusive = false) {
            secondExecuted = System.nanoTime()
            println("Second executed")
          }.await
        }.await(2.seconds)

        //second will only occur after first
        future.isCompleted shouldBe true

        firstExecuted should be < secondExecuted

        future.await
        ranges.isEmpty shouldBe true
      }

      "exist is non-exclusive" in {
        runThis(10.times, log = true) {
          Seq(true, false) foreach {
            secondToInclusive =>
              println(s"\nToInclusive: $secondToInclusive")

              implicit val ranges = AtomicRanges[Int]()

              @volatile var firstExecuted: Long = Long.MaxValue
              @volatile var secondExecuted: Long = Long.MaxValue

              //execute first - [10 - 20]
              val first =
                Future {
                  ranges
                    .execute(fromKey = 10, toKey = 20, toKeyInclusive = false) {
                      sleep(3.second)
                      firstExecuted = System.nanoTime()
                      println("First executed")
                    }
                }

              //wait for the reserve to occur
              eventual(!ranges.isEmpty)

              //execute second - [20 - 30] //20 is exclusive in first execution
              Future {
                ranges.execute(fromKey = 20, toKey = 30, toKeyInclusive = secondToInclusive) {
                  secondExecuted = System.nanoTime()
                  println("Second executed")
                }.await
              }.await //second is complete

              secondExecuted should be < firstExecuted

              //first is still running
              first.isCompleted shouldBe false
              ranges.isEmpty shouldBe false
              ranges.size shouldBe 1

              eventual(5.seconds) {
                !ranges.isEmpty
                first.isCompleted shouldBe true
              }
          }
        }
      }
    }

    "overlaps" in {
      //initial range to reserve
      val rangeToReserve = (10, 15)

      //other ranges to check for atomicity
      val rangesToCheck =
        (1 to 15) flatMap {
          from =>
            (10 to 20) map {
              to =>
                (from, to)
            }
        }

      rangesToCheck foreach {
        case (from, to) =>
          println(s"\nRange check: $from - $to")
          implicit val ranges = AtomicRanges[Int]()

          @volatile var firstExecuted: Long = 0
          @volatile var secondExecuted: Long = 0

          //execute first
          Future {
            ranges
              .execute(fromKey = rangeToReserve._1, toKey = rangeToReserve._2, toKeyInclusive = true) {
                sleep(50.milliseconds)
                firstExecuted = System.nanoTime()
                println("First executed")
              }.await
          }

          val future2 =
            Future {
              ranges.execute(fromKey = from, toKey = to, toKeyInclusive = true) {
                secondExecuted = System.nanoTime()
                println("Second executed")
              }.await
            }

          eventual {
            firstExecuted should be < secondExecuted
          }

          future2.await
          eventual(ranges.isEmpty shouldBe true)
      }
    }
  }

  "release on failure" when {
    "overlaps" in {
      runThis(10.times, log = true) {
        implicit val ranges = AtomicRanges[Int]()

        //create random overlaps
        val from = randomIntMax(10)
        val to = from + randomIntMax(10)

        //execute first
        Future {
          ranges
            .execute(fromKey = from, toKey = to, toKeyInclusive = true) {
              eitherOne(sleep(randomIntMax(20).milliseconds), ()) //sleep optionally
              throw new Exception("Failed one")
            }.await
        }

        val from2 = randomIntMax(10)
        val to2 = from2 + randomIntMax(10)

        Future {
          ranges.execute(fromKey = from2, toKey = to2, toKeyInclusive = true) {
            eitherOne(sleep(randomIntMax(20).milliseconds), ()) //sleep optionally
            throw new Exception("Failed two")
          }.await
        }

        eventual(ranges.isEmpty shouldBe true)
        sleep(1.second)
        eventual(ranges.isEmpty shouldBe true)
      }
    }
  }

}

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

import java.util.concurrent.ConcurrentSkipListMap

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Bag.Glass
import swaydb.IO.ApiIO
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.TestExecutionContext
import swaydb.data.RunThis._
import swaydb.{Bag, IO}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Random, Try}

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

class AtomicRanges_LessSpec extends AtomicRangesSpec[Bag.Glass] {

  override def get[A](a: Bag.Glass[A]): A = a

  override def getException(a: => Glass[_]): Throwable =
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

  def readOrWriteAction(): AtomicRanges.Action =
    eitherOne(new AtomicRanges.Action.Read, AtomicRanges.Action.Write)

  "ordering" when {
    "write on write with same keys" in {
      runThis(10.times) {
        val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

        val key = new AtomicRanges.Key(1, 10, true, readOrWriteAction())
        val value = new AtomicRanges.Value(0)

        map.put(key, value)
        map.put(key, value)

        map.size() shouldBe 1
        map.get(key) shouldBe value
      }
    }

    "write on write with overlapping ranges" in {
      runThis(10.times) {
        val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

        //these keys are overlapping
        val key1 = new AtomicRanges.Key(5, 10, true, AtomicRanges.Action.Write)
        val key2 = new AtomicRanges.Key(1, 5, true, AtomicRanges.Action.Write)

        val value1 = new AtomicRanges.Value(1)
        val value2 = new AtomicRanges.Value(2)

        map.put(key1, value1)
        map.put(key2, value2)

        map.size() shouldBe 1
        map.get(key1) shouldBe value2
        map.get(key2) shouldBe value2
      }
    }

    "write on write with non-v=overlapping ranges" in {
      runThis(10.times) {
        val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

        val key1 = new AtomicRanges.Key(5, 10, true, AtomicRanges.Action.Write)
        val key2 = new AtomicRanges.Key(1, 4, true, readOrWriteAction())

        val value1 = new AtomicRanges.Value(1)
        val value2 = new AtomicRanges.Value(2)

        map.put(key1, value1)
        map.put(key2, value2)

        map.size() shouldBe 2
        map.get(key1) shouldBe value1
        map.get(key2) shouldBe value2
      }
    }

    "read on read with same keys will yield different" in {
      val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

      val key1 = new AtomicRanges.Key(1, 10, true, new AtomicRanges.Action.Read)
      val key2 = new AtomicRanges.Key(1, 10, true, new AtomicRanges.Action.Read)

      val value1 = new AtomicRanges.Value(1)
      val value2 = new AtomicRanges.Value(2)

      map.put(key1, value1)
      map.put(key2, value2)

      map.size() shouldBe 2
      map.get(key1) shouldBe value1
      map.get(key2) shouldBe value2
    }

    "read on write" in {
      val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

      val readKey1 = new AtomicRanges.Key(1, 10, true, new AtomicRanges.Action.Read())
      val readKey2 = new AtomicRanges.Key(1, 10, true, new AtomicRanges.Action.Read())
      val value1 = new AtomicRanges.Value(1)
      val value2 = new AtomicRanges.Value(2)

      val writeKey = new AtomicRanges.Key(1, 10, true, AtomicRanges.Action.Write)
      val value3 = new AtomicRanges.Value(3)

      //insert read keys
      map.put(readKey1, value1)
      map.put(readKey2, value2)

      map.size() shouldBe 2

      //it contains writeKey since writeKey and read keys are same
      map.containsKey(writeKey) shouldBe true

      //remove readKey1 and it still contains write key
      map.remove(readKey1)
      map.containsKey(writeKey) shouldBe true

      //remove readKey2 results in an empty map now the contains returns false
      map.remove(readKey2)
      map.isEmpty shouldBe true
      map.containsKey(writeKey) shouldBe false

      //insert the write key this time and check for read key.
      map.put(writeKey, value3)
      map.containsKey(writeKey) shouldBe true
      map.containsKey(readKey1) shouldBe true
      map.containsKey(readKey2) shouldBe true
    }

    "writes occur on existing ranges" when {
      def doTest(keys: Seq[Int], newAction: => AtomicRanges.Action) = {
        val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

        //inserts - [0 - 5], [5 - 10], [10 - 15] ...
        keys foreach {
          i =>
            if (i % 5 == 0) {
              val key = new AtomicRanges.Key(i, i + 5, false, newAction)
              val value = new AtomicRanges.Value(i)
              map.put(key, value)
            }
        }

        keys foreach {
          i =>
            val key = new AtomicRanges.Key(i, i, true, AtomicRanges.Action.Write)
            map.containsKey(key) shouldBe true
            map.putIfAbsent(key, new AtomicRanges.Value(i)) should not be null
        }
      }

      val keys = (0 to 100000).toList

      "existing write ranges" when {
        "inserted sequentially" in {
          doTest(keys, AtomicRanges.Action.Write)
        }

        "inserted randomly" in {
          doTest(Random.shuffle(keys), AtomicRanges.Action.Write)
        }
      }

      "existing read ranges" when {
        "inserted sequentially" in {
          doTest(keys, new AtomicRanges.Action.Read())
        }

        "inserted randomly" in {
          doTest(Random.shuffle(keys), new AtomicRanges.Action.Read())
        }
      }
    }

    "reads occur on existing read ranges" when {
      def doTest(keys: Seq[Int]) = {
        val map = new ConcurrentSkipListMap[AtomicRanges.Key[Int], AtomicRanges.Value[Int]](AtomicRanges.Key.order(Ordering.Int))

        //inserts - [0 - 5], [5 - 10], [10 - 15] ...
        keys foreach {
          i =>
            if (i % 5 == 0) {
              val key = new AtomicRanges.Key(i, i + 5, false, new AtomicRanges.Action.Read())
              val value = new AtomicRanges.Value(i)
              map.put(key, value)
            }
        }

        keys foreach {
          i =>
            val key = new AtomicRanges.Key(i, i, true, new AtomicRanges.Action.Read())
            map.containsKey(key) shouldBe false
            map.putIfAbsent(key, new AtomicRanges.Value(i)) shouldBe null
        }
      }

      val keys = (0 to 100000).toList

      "inserted sequentially" in {
        doTest(keys)
      }

      "inserted randomly" in {
        doTest(Random.shuffle(keys))
      }
    }
  }

  "execute" when {
    "no overlaps" in {
      implicit val atomic = AtomicRanges[Int]()

      @volatile var executed = false

      atomic
        .write(fromKey = 1, toKey = 10, toKeyInclusive = randomBoolean()) {
          executed = true
        }
        .await

      executed shouldBe true

      atomic.isEmpty shouldBe true
    }

    "overlaps" when {
      "existing is exclusive" in {
        runThis(10.times, log = true) {
          implicit val ranges = AtomicRanges[Int]()

          @volatile var firstExecuted: Long = 0
          @volatile var secondExecuted: Long = 0

          //execute first
          val future =
            Future {
              ranges
                .write(fromKey = 10, toKey = 20, toKeyInclusive = true) {
                  sleep(1.second)
                  firstExecuted = System.nanoTime()
                  println("First executed")
                }
            }

          sleep(10.milliseconds)

          Future {
            def doWrite() =
              ranges.write(fromKey = 20, toKey = 30, toKeyInclusive = false) {
                secondExecuted = System.nanoTime()
                println("Second executed")
              }.await

            def doRead() =
              ranges.read[Int, Int, BAG](key => key, Int.MinValue) {
                secondExecuted = System.nanoTime()
                println("Second executed")
                Random.shuffle(List.range(10, 20)).head
              }.await

            eitherOne(doWrite(), doRead())
          }.await(2.seconds)

          //second will only occur after first
          future.isCompleted shouldBe true

          firstExecuted should be < secondExecuted

          future.await
          ranges.isEmpty shouldBe true
        }
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
                    .write(fromKey = 10, toKey = 20, toKeyInclusive = false) {
                      sleep(3.second)
                      firstExecuted = System.nanoTime()
                      println("First executed")
                    }
                }

              //wait for the reserve to occur
              eventual(!ranges.isEmpty)

              Future {
                //execute second - [20 - 30] //20 is exclusive in first execution
                def doWrite() =
                  ranges.write(fromKey = 20, toKey = 30, toKeyInclusive = secondToInclusive) {
                    secondExecuted = System.nanoTime()
                    println("Second executed")
                  }.await

                def doRead() =
                  ranges.read[Int, Int, BAG](key => key, Int.MinValue) {
                    secondExecuted = System.nanoTime()
                    println("Second executed")
                    eitherOne(20, 21)
                  }.await

                eitherOne(doWrite(), doRead())
              }.await(2.seconds)

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
          val future1 =
            Future {

              def doWrite() =
                ranges.write(fromKey = rangeToReserve._1, toKey = rangeToReserve._2, toKeyInclusive = true) {
                  sleep(50.milliseconds)
                  firstExecuted = System.nanoTime()
                  println(s"First executed. Write. from = ${rangeToReserve._1}, to = ${rangeToReserve._2}")
                }.await

              def doRead() =
                ranges.read[Int, Int, BAG](key => key, Int.MinValue) {
                  firstExecuted = System.nanoTime()
                  val readInteger = eitherOne(from, to)
                  println(s"First executed. Read. int = $readInteger")
                  readInteger
                }.await should not be Int.MaxValue

              eitherOne(doWrite(), doRead())
            }

          sleep(10.milliseconds)

          val future2 =
            Future {
              ranges.write(fromKey = from, toKey = to, toKeyInclusive = true) {
                secondExecuted = System.nanoTime()
                println("Second executed")
              }.await
            }

          eventual {
            firstExecuted should be < secondExecuted
          }

          future2.await
          eventual(ranges.isEmpty shouldBe true)
          future1.isCompleted shouldBe true
      }
    }
  }

  "release on failure" when {
    "overlaps" in {
      runThis(10.times, log = true) {
        implicit val ranges = AtomicRanges[Int]()

        def doWrite(from: Int, to: Int) =
          ranges
            .write(fromKey = from, toKey = to, toKeyInclusive = true) {
              eitherOne(sleep(randomIntMax(5).milliseconds), ()) //sleep optionally
              throw new Exception("Failed one")
            }.await

        def doRead() =
          ranges
            .read[Int, Int, BAG](int => int, Int.MinValue) {
              eitherOne(sleep(randomIntMax(5).milliseconds), ()) //sleep optionally
              throw new Exception("Failed one")
            }.await should not be Int.MinValue

        //create random overlaps
        val from = randomIntMax(10)
        val to = from + randomIntMax(10)

        //execute first
        Future {
          eitherOne(doWrite(from, to), doRead())
        }

        val from2 = randomIntMax(10)
        val to2 = from2 + randomIntMax(10)

        Future {
          eitherOne(doWrite(from2, to2), doRead())
        }

        eventual(ranges.isEmpty shouldBe true)
        sleep(1.second)
        eventual(ranges.isEmpty shouldBe true)
      }
    }
  }

}

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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Bag._
import swaydb.IO.ApiIO
import swaydb.core.TestExecutionContext
import swaydb.data.RunThis._
import swaydb.data.slice.Slice
import swaydb.{Bag, Glass, IO, Stream}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class StreamFutureSpec extends StreamSpec[Future]()(Bag.future(TestExecutionContext.executionContext)) {
  override def get[A](a: Future[A]): A = Await.result(a, 60.seconds)

  override def getException(a: => Future[_]): Throwable = {
    val future = try a finally {} //wait for the future to complete
    sleep(1.second)
    future.value.get.failed.get //get it's result
  }
}

class StreamIOSpec extends StreamSpec[IO.ApiIO] {
  override def get[A](a: IO.ApiIO[A]): A = a.get

  override def getException(a: => ApiIO[_]): Throwable =
    a.left.get.exception
}

class StreamLessSpec extends StreamSpec[Glass] {
  override def get[A](a: Glass[A]): A = a

  override def getException(a: => Glass[_]): Throwable =
    IO(a).left.get
}

class StreamTrySpec extends StreamSpec[Try] {
  override def get[A](a: Try[A]): A = a.get

  override def getException(a: => Try[_]): Throwable =
    a.failed.get
}

sealed abstract class StreamSpec[BAG[_]](implicit bag: Bag[BAG]) extends AnyWordSpec with Matchers {

  def get[A](a: BAG[A]): A

  def getException(a: => BAG[_]): Throwable

  implicit class Get[A](a: BAG[A]) {
    def await = get(a)
  }

  val failureIterator =
    new Iterator[Int] {
      override def hasNext: Boolean =
        throw new Exception("Failed hasNext")

      override def next(): Int =
        throw new Exception("Failed next")
    }

  "Stream" should {

    "empty" in {
      def stream =
        Stream
          .empty[Int, BAG]
          .map(_.toString)

      stream
        .materialize
        .await shouldBe empty

      stream
        .materialize(List.newBuilder)
        .await shouldBe empty
    }

    "range" in {
      def intStream =
        Stream
          .range[BAG](1, 100)

      def charStream =
        Stream
          .range[BAG]('a', 'z')

      intStream
        .materialize
        .await shouldBe (1 to 100)

      charStream
        .materialize
        .await shouldBe ('a' to 'z')

      //from builders
      intStream
        .materialize(List.newBuilder)
        .await shouldBe (1 to 100)

      charStream
        .materialize(List.newBuilder)
        .await shouldBe ('a' to 'z')
    }

    "rangeUntil" in {
      Stream
        .rangeUntil[BAG](1, 100)
        .materialize
        .await shouldBe (1 to 99)

      Stream
        .rangeUntil[BAG]('a', 'z')
        .materialize
        .await shouldBe ('a' to 'y')
    }

    "tabulate" in {
      Stream
        .tabulate[Int, BAG](5)(_ + 1)
        .materialize
        .await shouldBe (1 to 5)

      Stream
        .tabulate[Int, BAG](0)(_ + 1)
        .materialize
        .await shouldBe empty
    }

    "headOption" in {
      Stream[Int, BAG](1 to 100)
        .head
        .await should contain(1)
    }

    "headOption failure" in {
      def stream =
        Stream[Int, BAG](failureIterator)
          .head

      getException(stream).getMessage shouldBe "Failed hasNext"
    }

    "take" in {
      Stream[Int, BAG](1, 2)
        .map(_ + 1)
        .take(1)
        .materialize
        .await shouldBe List(2)
    }

    "count" in {
      Stream[Int, BAG](1 to 100)
        .count(_ % 2 == 0)
        .await shouldBe 50
    }

    "lastOptionLinear" in {
      Stream[Int, BAG](1 to 100)
        .last
        .await should contain(100)
    }

    "lastOption failure" in {
      def stream =
        Stream[Int, BAG](failureIterator)
          .last

      getException(stream).getMessage shouldBe "Failed hasNext"
    }

    "map" in {
      def mapStream =
        Stream[Int, BAG](1 to 1000)
          .map(_ + " one")
          .map(_ + " two")
          .map(_ + " three")

      mapStream
        .materialize
        .await shouldBe (1 to 1000).map(_ + " one two three")

      mapStream
        .materialize(Slice.newBuilder(1)).await shouldBe a[Slice[Int]]

      mapStream
        .materialize(List.newBuilder)
        .await shouldBe (1 to 1000).map(_ + " one two three")
    }

    "collect conditional" in {
      Stream[Int, BAG](1 to 1000)
        .collect { case n if n % 2 == 0 => n }
        .materialize
        .await shouldBe (2 to 1000 by 2)
    }

    "collectFirst" in {
      Stream[Int, BAG](1 to 1000)
        .collectFirst { case n if n % 2 == 0 => n }
        .await should contain(2)
    }

    "collect all" in {
      Stream[Int, BAG](1 to 1000)
        .collect { case n => n }
        .materialize
        .await shouldBe (1 to 1000)
    }

    "drop, take and map" in {
      def stream =
        Stream[Int, BAG](1 to 20)
          .map(_.toString)
          .drop(10)
          .take(5)
          .map(_.toInt)
          .drop(2)
          .take(1)

      stream
        .materialize
        .await should contain only 13

      val sliceBuilder = stream.materialize(Slice.newBuilder(20))

      sliceBuilder
        .await shouldBe a[Slice[Int]]

      sliceBuilder
        .await should contain only 13
    }

    "drop, take" in {
      Stream[Int, BAG](1 to 1000)
        .map(_.toString)
        .drop(10)
        .take(1)
        .map(_.toInt)
        .materialize
        .await should have size 1
    }

    "foreach" in {
      var foreachItems = ListBuffer.empty[Int]

      Stream[Int, BAG](1 to 1000)
        .map(_.toString)
        .drop(0)
        .take(1000)
        .map(_.toInt)
        .foreach(foreachItems += _)
        .await shouldBe (())

      foreachItems shouldBe (1 to 1000)
    }

    "takeWhile" in {
      Stream[Int, BAG](1 to 1000)
        .map(_.toString)
        .drop(10)
        .takeWhile(_.toInt <= 10)
        .materialize
        .await shouldBe empty
    }

    "dropWhile" in {
      Stream[Int, BAG](1 to 20)
        .map(_.toString)
        .drop(10)
        .dropWhile(_.toInt <= 20)
        .materialize
        .await shouldBe empty

      Stream[Int, BAG](1 to 20)
        .map(_.toString)
        .drop(11)
        .dropWhile(_.toInt % 2 == 0)
        .materialize
        .await shouldBe (13 to 20).map(_.toString)
    }

    "flatMap" in {
      Stream[Int, BAG](1 to 10)
        .flatMap(_ => Stream[Int, BAG](1 to 10))
        .materialize
        .await shouldBe Array.fill(10)(1 to 10).flatten
    }

    "filter" in {
      Stream[Int, BAG](1 to 10)
        .map(_ + 10)
        .filter(_ % 2 == 0)
        .take(2)
        .materialize
        .await should contain only(12, 14)
    }

    "filterNot" in {
      Stream[Int, BAG](1 to 10)
        .filterNot(_ % 2 == 0)
        .take(2)
        .materialize
        .await shouldBe (1 to 10).filter(_ % 2 != 0).take(2)
    }

    "partition" in {
      val (leftStream, rightStream) =
        Stream[Int, BAG](1 to 10)
          .partition(_ % 2 == 0)
          .await

      leftStream shouldBe (1 to 10).filter(_ % 2 == 0)
      rightStream shouldBe (1 to 10).filterNot(_ % 2 == 0)
    }

    "not stack overflow" in {
      Stream[Int, BAG](1 to 100000)
        .filter(_ % 10000 == 0)
        .materialize
        .await should contain only(10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)
    }

    "sum" in {
      val range = 1 to 100

      Stream[Int, BAG](range)
        .sum
        .await shouldBe range.sum
    }

    "failure should terminate the Stream" in {
      def matStream =
        Stream[Int, BAG](1 to 1000)
          .map {
            int =>
              if (int == 100)
                throw new Exception(s"Failed at $int")
              else
                int
          }
          .materialize

      def foreachStream =
        Stream[Int, BAG](1 to 1000)
          .foreach {
            int =>
              if (int == 100)
                throw new Exception(s"Failed at $int")
              else
                int
          }

      Seq(
        getException(matStream),
        getException(foreachStream)
      ) foreach {
        exception =>
          exception.getMessage shouldBe "Failed at 100"
      }
    }

    "flatten" when {
      "success" in {
        val stream: Stream[BAG[Int], BAG] = Stream(bag(1), bag(2), bag(3), bag(4))

        val flat: Stream[Int, BAG] = stream.flatten

        flat.materialize.await shouldBe List(1, 2, 3, 4)
      }

      "failed" in {
        if (bag == Bag.glass) {
          cancel("Test does not apply to Bag.Less as it throws Exceptions")
        } else {
          val stream: Stream[BAG[Int], BAG] = Stream(bag(1), bag(2), bag(3), bag(4), bag.failure(new Exception("oh no")))

          val flat: Stream[Int, BAG] = stream.flatten

          Try(flat.materialize.await).failed.get.getMessage shouldBe "oh no"
        }
      }
    }

    "foldLeftBags" when {
      "success" in {
        val stream: Stream[Int, BAG] = Stream.range[BAG](1, 10)

        val fold: BAG[Int] =
          stream.foldLeftFlatten(0) {
            case (left, right) =>
              bag(left + right)
          }

        fold.await shouldBe 55
      }

      "failed" in {
        if (bag == Bag.glass) {
          cancel("Test does not apply to Bag.Less as it throws Exceptions")
        } else {
          val stream: Stream[Int, BAG] = Stream.range[BAG](1, 10)

          val fold: BAG[Int] =
            stream.foldLeftFlatten(0) {
              case (left, right) =>
                if (right == 9)
                  throw new Exception("oh no")
                else
                  bag(left + right)
            }

          Try(fold.await).failed.get.getMessage shouldBe "oh no"
        }
      }
    }

    "mapBags" when {
      "success" in {
        val stream: Stream[Int, BAG] = Stream.range[BAG](1, 10)

        val fold: Stream[Int, BAG] =
          stream.mapFlatten {
            int =>
              bag(int)
          }

        fold.materialize.await.toList shouldBe (1 to 10).toList
      }

      "failed" in {
        if (bag == Bag.glass) {
          cancel("Test does not apply to Bag.Less as it throws Exceptions")
        } else {
          val stream: Stream[Int, BAG] = Stream.range[BAG](1, 10)

          val fold: Stream[BAG[Int], BAG] =
            stream.map {
              int =>
                if (int == 9)
                  throw new Exception("oh no")
                else
                  bag(int)
            }

          Try(fold.materialize.await).failed.get.getMessage shouldBe "oh no"
        }
      }
    }
  }
}

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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.api

import org.scalatest.{Matchers, WordSpec}
import swaydb.Bag._
import swaydb.IO.ApiIO
import swaydb.core.RunThis._
import swaydb.{Bag, IO, Stream}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class StreamFutureSpec extends StreamSpec[Future] {
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

class StreamBagLessSpec extends StreamSpec[Bag.Less] {
  override def get[A](a: Bag.Less[A]): A = a

  override def getException(a: => Less[_]): Throwable =
    IO(a).left.get
}

class StreamTrySpec extends StreamSpec[Try] {
  override def get[A](a: Try[A]): A = a.get

  override def getException(a: => Try[_]): Throwable =
    a.failed.get
}

sealed abstract class StreamSpec[BAG[_]](implicit bag: Bag[BAG]) extends WordSpec with Matchers {

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
      Stream.empty[Int]
        .map(_.toString)
        .materialize[BAG]
        .await shouldBe empty
    }

    "range" in {
      Stream
        .range(1, 100)
        .materialize[BAG]
        .await shouldBe (1 to 100)

      Stream
        .range('a', 'z')
        .materialize[BAG]
        .await shouldBe ('a' to 'z')
    }

    "rangeUntil" in {
      Stream
        .rangeUntil(1, 100)
        .materialize[BAG]
        .await shouldBe (1 to 99)

      Stream
        .rangeUntil('a', 'z')
        .materialize[BAG]
        .await shouldBe ('a' to 'y')
    }

    "tabulate" in {
      Stream
        .tabulate[Int](5)(_ + 1)
        .materialize[BAG]
        .await shouldBe (1 to 5)

      Stream
        .tabulate[Int](0)(_ + 1)
        .materialize[BAG]
        .await shouldBe empty
    }

    "headOption" in {
      Stream[Int](1 to 100)
        .headOption[BAG]
        .await should contain(1)
    }

    "headOption failure" in {
      def stream =
        Stream[Int](failureIterator)
          .headOption[BAG]

      getException(stream).getMessage shouldBe "Failed hasNext"
    }

    "take" in {
      Stream[Int](1, 2)
        .map(_ + 1)
        .take(1)
        .materialize[BAG]
        .await shouldBe List(2)
    }

    "count" in {
      Stream[Int](1 to 100)
        .count[BAG](_ % 2 == 0)
        .await shouldBe 50
    }

    "lastOptionLinear" in {
      Stream[Int](1 to 100)
        .lastOption[BAG]
        .await should contain(100)
    }

    "lastOption failure" in {
      def stream =
        Stream[Int](failureIterator)
          .lastOption[BAG]

      getException(stream).getMessage shouldBe "Failed hasNext"
    }

    "map" in {
      Stream[Int](1 to 1000)
        .map(_ + " one")
        .map(_ + " two")
        .map(_ + " three")
        .materialize[BAG]
        .await shouldBe (1 to 1000).map(_ + " one two three")
    }

    "collect conditional" in {
      Stream[Int](1 to 1000)
        .collect { case n if n % 2 == 0 => n }
        .materialize[BAG]
        .await shouldBe (2 to 1000 by 2)
    }

    "collectFirst" in {
      Stream[Int](1 to 1000)
        .collectFirst[Int, BAG] { case n if n % 2 == 0 => n }
        .await should contain(2)
    }

    "collect all" in {
      Stream[Int](1 to 1000)
        .collect { case n => n }
        .materialize[BAG]
        .await shouldBe (1 to 1000)
    }

    "drop, take and map" in {
      Stream[Int](1 to 20)
        .map(_.toString)
        .drop(10)
        .take(5)
        .map(_.toInt)
        .drop(2)
        .take(1)
        .materialize[BAG]
        .await should contain only 13
    }

    "drop, take" in {
      Stream[Int](1 to 1000)
        .map(_.toString)
        .drop(10)
        .take(1)
        .map(_.toInt)
        .materialize[BAG]
        .await should have size 1
    }

    "foreach" in {
      var foreachItems = ListBuffer.empty[Int]

      Stream[Int](1 to 1000)
        .map(_.toString)
        .drop(0)
        .take(1000)
        .map(_.toInt)
        .foreach[BAG](foreachItems += _)
        .await shouldBe (())

      foreachItems shouldBe (1 to 1000)
    }

    "takeWhile" in {
      Stream[Int](1 to 1000)
        .map(_.toString)
        .drop(10)
        .takeWhile(_.toInt <= 10)
        .materialize[BAG]
        .await shouldBe empty
    }

    "dropWhile" in {
      Stream[Int](1 to 20)
        .map(_.toString)
        .drop(10)
        .dropWhile(_.toInt <= 20)
        .materialize[BAG]
        .await shouldBe empty

      Stream[Int](1 to 20)
        .map(_.toString)
        .drop(11)
        .dropWhile(_.toInt % 2 == 0)
        .materialize[BAG]
        .await shouldBe (13 to 20).map(_.toString)
    }

    "flatMap" in {
      Stream[Int](1 to 10)
        .flatMap(_ => Stream[Int](1 to 10))
        .materialize[BAG]
        .await shouldBe Array.fill(10)(1 to 10).flatten
    }

    "filter" in {
      Stream[Int](1 to 10)
        .map(_ + 10)
        .filter(_ % 2 == 0)
        .take(2)
        .materialize[BAG]
        .await should contain only(12, 14)
    }

    "filterNot" in {
      Stream[Int](1 to 10)
        .filterNot(_ % 2 == 0)
        .take(2)
        .materialize[BAG]
        .await shouldBe (1 to 10).filter(_ % 2 != 0).take(2)
    }

    "partition" in {
      Stream[Int](1 to 10)
        .partition(_ % 2 == 0)
        ._1
        .materialize[BAG]
        .await shouldBe (1 to 10).filter(_ % 2 == 0)

      Stream[Int](1 to 10)
        .partition(_ % 2 == 0)
        ._2
        .materialize[BAG]
        .await shouldBe (1 to 10).filter(_ % 2 != 0)
    }

    "not stack overflow" in {
      Stream[Int](1 to 1000000)
        .filter(_ % 100000 == 0)
        .materialize[BAG]
        .await should contain only(100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000)
    }

    "sum" in {
      val range = 1 to 100

      Stream[Int](range)
        .sum[BAG]
        .await shouldBe range.sum
    }

    "failure should terminate the Stream" in {
      def matStream =
        Stream[Int](1 to 1000)
          .map {
            int =>
              if (int == 100)
                throw new Exception(s"Failed at $int")
              else
                int
          }
          .materialize[BAG]

      def foreachStream =
        Stream[Int](1 to 1000)
          .foreach[BAG] {
            int =>
              if (int == 100)
                throw new Exception(s"Failed at $int")
              else
                int
          }

      Seq(
        getException(matStream),
        getException(foreachStream),
      ) foreach {
        exception =>
          exception.getMessage shouldBe "Failed at 100"
      }
    }
  }
}

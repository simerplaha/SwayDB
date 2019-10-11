/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
import swaydb.Tag._
import swaydb.core.RunThis._
import swaydb.{IO, Stream, Tag}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

class StreamFutureSpec extends StreamSpec[Future] {
  override def get[A](a: Future[A]): A = Await.result(a, 60.seconds)
}

class StreamIOSpec extends StreamSpec[IO.ApiIO] {
  override def get[A](a: IO.ApiIO[A]): A = a.get
}

class StreamTrySpec extends StreamSpec[Try] {
  override def get[A](a: Try[A]): A = a.get
}

sealed abstract class StreamSpec[T[_]](implicit tag: Tag[T]) extends WordSpec with Matchers {

  def get[A](a: T[A]): A

  implicit class Get[A](a: T[A]) {
    def await = get(a)
  }

  "Stream" should {

    "empty" in {
      Stream.empty[Int, T]
        .map(_.toString)
        .materialize
        .await shouldBe empty
    }

    "headOption" in {
      Stream[Int, T](1 to 100)
        .headOption
        .await should contain(1)
    }

    "lastOptionLinear" in {
      Stream[Int, T](1 to 100)
        .lastOption
        .await should contain(100)
    }

    "map" in {
      Stream[Int, T](1 to 1000)
        .map(_ + " one")
        .map(_ + " two")
        .map(_ + " three")
        .materialize
        .await shouldBe (1 to 1000).map(_ + " one two three")
    }

    "collect conditional" in {
      Stream[Int, T](1 to 1000)
        .collect { case n if n % 2 == 0 => n }
        .materialize
        .await shouldBe (2 to 1000 by 2)
    }

    "collect all" in {
      Stream[Int, T](1 to 1000)
        .collect { case n => n }
        .materialize
        .await shouldBe (1 to 1000)
    }

    "drop, take and map" in {
      Stream[Int, T](1 to 20)
        .map(_.toString)
        .drop(10)
        .take(5)
        .map(_.toInt)
        .drop(2)
        .take(1)
        .materialize
        .await should contain only 13
    }

    "drop, take and foreach" in {
      Stream[Int, T](1 to 1000)
        .map(_.toString)
        .drop(10)
        .take(1)
        .map(_.toInt)
        .materialize
        .await should have size 1
    }

    "takeWhile" in {
      Stream[Int, T](1 to 1000)
        .map(_.toString)
        .drop(10)
        .takeWhile(_.toInt <= 10)
        .materialize
        .await shouldBe empty
    }

    "dropWhile" in {
      Stream[Int, T](1 to 20)
        .map(_.toString)
        .drop(10)
        .dropWhile(_.toInt <= 20)
        .materialize
        .await shouldBe empty

      Stream[Int, T](1 to 20)
        .map(_.toString)
        .drop(11)
        .dropWhile(_.toInt % 2 == 0)
        .materialize
        .await shouldBe (13 to 20).map(_.toString)
    }

    "flatMap" in {
      Stream[Int, T](1 to 10)
        .flatMap(_ => Stream[Int, T](1 to 10))
        .materialize
        .await shouldBe Array.fill(10)(1 to 10).flatten
    }

    "filter" in {
      Stream[Int, T](1 to 10)
        .map(_ + 10)
        .filter(_ % 2 == 0)
        .take(2)
        .materialize
        .await should contain only(12, 14)
    }

    "filterNot" in {
      Stream[Int, T](1 to 10)
        .filterNot(_ % 2 == 0)
        .take(2)
        .materialize
        .await shouldBe (1 to 10).filter(_ % 2 != 0).take(2)
    }

    "not stack overflow" in {
      Stream[Int, T](1 to 1000000)
        .filter(_ % 100000 == 0)
        .materialize
        .await should contain only(100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000)
    }
  }
}

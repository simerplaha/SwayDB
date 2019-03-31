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

package swaydb

import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.Future
import scala.util.Try
import swaydb.Wrap._
import swaydb.core.RunThis._
import swaydb.data.IO

class StreamSpec extends WordSpec with Matchers {

  "Stream" should {

    "iterate future" in {

      def stream =
        Stream[Int, Future](1 to 1000)
          .map(_ + " one")
          .map(_ + " two")
          .map(_ + " three")

      def assert() =
        stream.toSeq.await shouldBe (1 to 1000).map(_ + " one two three")

      assert()
      assert() //assert again, streams can be re-read.

      stream.foldLeft(0)(_ + _.takeWhile(_.isDigit).toInt).await shouldBe (1 to 1000).sum
      stream.lastOptionLinear.await.get shouldBe "1000 one two three"

      stream.filter(_.contains("00")).toSeq.await should have size 10

      Stream[Int, Future](1 to 10).flatMap(_ => Stream[Int, Future](1 to 10)).await.toSeq.await shouldBe Array.fill(10)(1 to 10).flatten

    }

    "try" in {

      def stream =
        Stream[Int, Try](1 to 1000)
          .map(_ + " one")
          .map(_ + " two")
          .map(_ + " three")

      def assert() =
        stream.toSeq.get shouldBe (1 to 1000).map(_ + " one two three")

      assert()
      assert() //assert again, streams can be re-read.

      stream.foldLeft(0)(_ + _.takeWhile(_.isDigit).toInt).get shouldBe (1 to 1000).sum
      stream.lastOptionLinear.get.get shouldBe "1000 one two three"

      stream.filter(_.contains("00")).toSeq.get should have size 10

      Stream[Int, Try](1 to 10).flatMap(_ => Stream[Int, Try](1 to 10)).get.toSeq.get shouldBe Array.fill(10)(1 to 10).flatten
    }

    "IO" in {

      def stream =
        Stream[Int, IO](1 to 1000)
          .map(_ + " one")
          .map(_ + " two")
          .map(_ + " three")

      def assert() =
        stream.toSeq.get shouldBe (1 to 1000).map(_ + " one two three")

      assert()
      assert() //assert again, streams can be re-read.

      stream.foldLeft(0)(_ + _.takeWhile(_.isDigit).toInt).get shouldBe (1 to 1000).sum
      stream.lastOptionLinear.get.get shouldBe "1000 one two three"

      stream.filter(_.contains("00")).toSeq.get should have size 10

      Stream[Int, IO](1 to 10).flatMap(_ => Stream[Int, IO](1 to 10)).get.toSeq.get shouldBe Array.fill(10)(1 to 10).flatten
    }
  }
}

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
import scala.util.{Success, Try}
import swaydb.Wrap._
import swaydb.core.RunThis._

class StreamSpec extends WordSpec with Matchers {

  "Stream" should {
    "iterate future" in {

      val futures =
        (1 to 100000000) map {
          i =>
            () => Future(i)
        }

      val stream = new Stream[Int, Future]() {
        val iterator = futures.iterator
        override def hasNext: Future[Boolean] = Future(iterator.hasNext)
        override def next(): Future[Int] = iterator.next()()
      }

      stream foreach {
        future =>
          if (future % 100000 == 0)
            println(future)
      }
      Thread.sleep(20000)
    }

    "try" in {

      val futures =
        (1 to 1000) map {
          i =>
            () => Success(i)
        }

      val stream = new Stream[Int, Try]() {
        val iterator = futures.iterator
        override def hasNext: Try[Boolean] = Success(iterator.hasNext)
        override def next(): Try[Int] = iterator.next()()
      }

      (stream ++ stream) foreach {
        future =>
          if (future % 100 == 0)
            println(future)
      }

    }
  }

}

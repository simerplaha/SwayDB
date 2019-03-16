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
        (1 to 100) map {
          i =>
            () => Future(i)
        }

      val stream = new Stream[Int, Future]() {
        val iterator = futures.iterator

        def step() =
          if (iterator.hasNext)
            iterator.next()().map(Some(_))
          else
            Future.successful(None)

        override def first(): Future[Option[Int]] = step()
        override def next(previous: Int): Future[Option[Int]] = step()
      }

      stream foreach {
        future =>
//          if (future % 100 == 0)
            println(future)
      }
      Thread.sleep(5000)
    }

    "try" in {

      val futures =
        (1 to 1000) map {
          i =>
            () => Success(i)
        }

      //      val stream = new Stream[Int, Try]() {
      //        val streamBuilder = futures.iterator
      //        override def hasNext: Try[Boolean] = Success(streamBuilder.hasNext)
      //        override def next(): Try[Int] = streamBuilder.next()()
      //      }
      //
      //      (stream ++ stream) foreach {
      //        future =>
      //          if (future % 100 == 0)
      //            println(future)
      //      }

    }
  }

}

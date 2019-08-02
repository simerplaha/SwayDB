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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

sealed trait IOValues {

  implicit class IOIncompleteImplicits[E: ErrorHandler, T](io: => IO[E, T]) {
    def runBlockingIO: IO[E, T] =
      IO.Deferred(io.get).runBlockingIO

    def runFutureIO: IO[E, T] =
      IO.Deferred(io.get).runFutureIO

    def runRandomIO: IO[E, T] =
      IO.Deferred(io.get).runRandomIO
  }

  implicit class IOImplicits[E: ErrorHandler, T](io: IO[E, T]) {
    def value: T =
      io.get
  }

  implicit class DeferredIOImplicits[E: ErrorHandler, T](io: => IO.Deferred[E, T]) {

    def runBlockingIO: IO[E, T] =
      io.runIO

    def runFutureIO: IO[E, T] =
      IO(Await.result(io.runFuture, 5.minutes))

    def runRandomIO: IO[E, T] =
      if (Random.nextBoolean())
        runBlockingIO
      else
        runFutureIO
  }
}

object IOValues extends IOValues

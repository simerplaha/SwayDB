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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

trait IOValues {

  implicit class SafeIO[T](io: => T) {
    def runRandomIO: IO[swaydb.Error.Segment, T] =
      IO.Defer[swaydb.Error.Segment, T](io).runRandomIO
  }

  implicit class IOIncompleteImplicits[L: IO.ExceptionHandler, R](io: => IO[L, R]) {
    def runBlockingIO: IO[L, R] =
      IO.Defer(io.get).runBlockingIO

    def runFutureIO: IO[L, R] =
      IO.Defer(io.get).runFutureIO

    def runRandomIO: IO[L, R] =
      IO.Defer(io.get).runRandomIO
  }

  implicit class IOImplicits[E, T](io: IO[E, T]) {
    def value: T =
      io.get
  }

  implicit class IOEitherImplicits[E, L, R](io: IO[E, Either[L, R]]) {
    def rightValue: R =
      io.get.right.get

    def leftValue: L =
      io.get.left.get
  }

  implicit class DeferredIOImplicits[L: IO.ExceptionHandler, R](io: => IO.Defer[L, R]) {

    def runBlockingIO: IO[L, R] =
      io.runIO

    def runFutureIO: IO[L, R] =
      IO(Await.result(io.run[R, Future](0), 5.minutes))

    def runRandomIO: IO[L, R] =
      if (Random.nextBoolean())
        runBlockingIO
      else
        runFutureIO
  }
}

object IOValues extends IOValues

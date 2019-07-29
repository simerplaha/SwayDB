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

  implicit class IOImplicits[E: ErrorHandler, T](io: IO[E, T]) {
    def value: T =
      io.get

    def valueIO: IO[E, T] =
      if (Random.nextBoolean())
        IO.Deferred[E, T](io.get).runIO
      else
        IO(Await.result(IO.Deferred[E, T](io.get).runFuture, 1.minute))
  }

  implicit class DeferredIOImplicits[E: ErrorHandler, T](io: => IO.Deferred[E, T]) {

    def valueIO: IO[E, T] =
      if (Random.nextBoolean())
        io.runIO
      else
        valueFutureIO

    def valueFutureIO: IO[E, T] =
      IO(Await.result(io.runFuture, 1.minute))
  }
}

object IOValues extends IOValues

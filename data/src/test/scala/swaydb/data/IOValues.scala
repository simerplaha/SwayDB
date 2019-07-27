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

package swaydb.data

import swaydb.{ErrorHandler, IO}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

sealed trait IOValues {

  implicit class RunIOImplicits[E: ErrorHandler, T](input: => IO[E, T]) {
    private[swaydb] def runIO: T =
      if (Random.nextBoolean())
        IO.Defer.recover[E, T](input.get).runBlocking.get
      else
        Await.result(IO.Defer.recover[E, T](input.get).runInFuture, 1.minute)
  }

  implicit class RunValueIOImplicits[E: ErrorHandler, T](input: IO[E, T]) {
    def value =
      input.get
  }

  implicit class RunDeferredIOImplicits[E: ErrorHandler, T](input: => IO.Defer[E, T]) {
    def runIO: T =
      if (Random.nextBoolean())
        input.runBlocking.get
      else
        Await.result(input.runInFuture, 1.minute)
  }
}

object IOValues extends IOValues

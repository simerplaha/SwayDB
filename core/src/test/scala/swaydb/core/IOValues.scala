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

package swaydb.core

import swaydb.IO
import swaydb.core.RunThis._
import swaydb.core.TestData.randomBoolean

import scala.concurrent.duration._

sealed trait IOValues {
  implicit class RunIOImplicits[T](input: => IO[T]) {
    private[core] def runIO: T =
      if (randomBoolean())
        IO.Defer.recover(input.get).runBlocking.get
      else
        IO.Defer.recover(input.get).runInFuture.await(1.minute)
  }

  implicit class RunValueIOImplicits[T](input: IO[T]) {
    def value =
      input.get
  }

  implicit class RunAsyncIOImplicits[T](input: => IO.Defer[T]) {
    def runIO: T =
      if (randomBoolean())
        input.runBlocking.get
      else
        input.runInFuture.await(1.minute)
  }
}

object IOValues extends IOValues

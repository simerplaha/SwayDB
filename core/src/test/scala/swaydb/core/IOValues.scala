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

import org.scalatest.Matchers
import org.scalatest.OptionValues._
import swaydb.core.RunThis._
import swaydb.core.TestData.randomBoolean
import swaydb.data.IO

import scala.concurrent.duration._

object IOValues extends Matchers {

  implicit class RunSafeOptionIOImplicits[T](input: => IO[Option[T]]) {
    def runIOValue: T =
      input.asAsync.runIO.value

    def runIO: Option[T] =
      input.asAsync.runIO
  }

  implicit class RunSafeIOImplicits[T](input: => IO[T]) {
    def runIO: T =
      input.asAsync.runIO

    def value =
      input.get
  }

  implicit class RunSafeAsyncIOImplicits[T](input: => IO.Async[T]) {
    def runIO: T =
      if (randomBoolean())
        IO.Async.runSafe(input.get).safeGetBlocking.get
      else
        IO.Async.runSafe(input.get).safeGetFuture.await(1.minute)
  }

  implicit class RunSafeAsyncIOOptionImplicits[T](input: => IO.Async[Option[T]]) {
    def runIOValue: T =
      input.runIO.value
  }
}

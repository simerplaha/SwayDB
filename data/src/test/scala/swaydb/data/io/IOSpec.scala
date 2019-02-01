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

package swaydb.data.io

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.exceptions.TestFailedException

class IOSpec extends WordSpec with Matchers with MockFactory {

  "Async" should {
    "flatMap on Success" in {
      val io =
        IO.Async(1).flatMapIO {
          int =>
            IO.Success(int + 1)
        }

      io.get shouldBe 2

      io.getOrListen(???).get shouldBe 2
    }

    "flatMap on Failure" in {
      val io: IO.Async[Int] =
        IO.Async(1).flatMapIO {
          _ =>
            IO.Failure(new TestFailedException("Kaboom!", 0))
        }

      assertThrows[TestFailedException] {
        io.get
      }

      io.getOrListen(???) shouldBe a[IO.Failure[_]]
    }

    "getOrListen on Failure" in {
      var returnFailure = true

      val io =
        IO.Async(1).flatMapIO {
          i =>
            if (returnFailure)
              IO.Failure(new TestFailedException("Kaboom!", 0))
            else
              IO.Success(i)
        }

      val listener = mockFunction[Int, Unit]
      listener expects 1 returning()
      io.getOrListen(listener) shouldBe a[IO.Failure[_]]
      returnFailure = false
      io.ready()
    }
  }
}

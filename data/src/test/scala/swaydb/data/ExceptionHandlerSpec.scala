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

import org.scalatest.{FlatSpec, Matchers}
import swaydb.IO.ExceptionHandler.Throwable
import swaydb._

class ExceptionHandlerSpec extends FlatSpec with Matchers {

  it should "Exceptions to Throwable" in {
    val exception = Exception.FailedToWriteAllBytes(0, 10, 10)

    val got: Throwable = IO.failed(exception = exception).left.get
    got shouldBe exception
  }

  it should "convert exception to typed Segment Errors" in {

    def assert(exception: Throwable) = {
      val io =
        IO[swaydb.Error.Segment, Unit] {
          throw exception
        }

      io.left.get shouldBe Error.DataAccess(Error.DataAccess.message, exception)
    }

    assert(new ArrayIndexOutOfBoundsException)
    assert(new IndexOutOfBoundsException)
    assert(new IllegalArgumentException)
    assert(new NegativeArraySizeException)

    val failedToWriteAllBytes = Exception.FailedToWriteAllBytes(10, 10, 10)

    val failure: IO[Throwable, Error.IO] =
      IO[swaydb.Error.IO, Unit] {
        throw failedToWriteAllBytes
      }.left

    failure.get shouldBe Error.FailedToWriteAllBytes(failedToWriteAllBytes)
  }
}

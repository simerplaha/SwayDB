/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.effect

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import swaydb.IO.ExceptionHandler.Throwable
import swaydb._

class ExceptionHandlerSpec extends AnyFlatSpec {

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

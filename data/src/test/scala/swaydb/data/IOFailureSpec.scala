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

import java.nio.file.{NoSuchFileException, Paths}

import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.data.Base._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class IOFailureSpec extends WordSpec with Matchers {

  "IO.Failure" should {
    "set boolean" in {
      val io = IO.Failure(IO.Error.OpeningFile(Paths.get(""), Reserve()))
      io.isFailure shouldBe true
      io.isLater shouldBe false
      io.isSuccess shouldBe false
    }

    "get" in {
      val io = IO.Failure[Int](new IllegalAccessError)

      assertThrows[IllegalAccessError] {
        io.get
      }
      io.safeGet shouldBe io
      io.safeGetBlocking shouldBe io
      Try(io.safeGetFuture.await).failed.get.getCause shouldBe a[IllegalAccessError]
    }

    "getOrElse & orElse return first io if both are Failures" in {
      val io1 = IO.Failure(new IllegalAccessError)
      val io2 = IO.Failure(new IllegalArgumentException)

      (io1 getOrElse io2).exception shouldBe a[IllegalArgumentException]

      io1 orElse io2 shouldBe io2
    }

    "flatMap on Success" in {
      val failIO = IO.Failure(new IllegalThreadStateException)
      failIO.asAsync flatMap {
        i =>
          IO.Success(1)
      } shouldBe failIO
    }

    "flatMap on failure" in {
      val failure = IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException("")))

      failure.asAsync flatMap {
        _ =>
          IO.Failure(new IllegalThreadStateException)
      } shouldBe failure
    }

    "flatten on successes with failure" in {
      val io = IO.Success(IO.Failure(IO.Error.Fatal(new Exception("Kaboom!"))))

      io.flatten.asInstanceOf[IO.Failure[Int]].exception.getMessage shouldBe "Kaboom!"
    }

    "flatten on failure with success" in {
      val io =
        IO.Failure(IO.Error.Fatal(new Exception("Kaboom!"))).asIO map {
          _ =>
            IO.Success(11)
        }

      io.flatten.asInstanceOf[IO.Failure[Int]].exception.getMessage shouldBe "Kaboom!"
    }

    "recover" in {
      val failure =
        IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException(""))) recover {
          case _ =>
            1
        }

      failure shouldBe IO.Success(1)
    }

    "recoverWith" in {
      val failure =
        IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException(""))) recoverWith {
          case _ =>
            IO.Failure(IO.Error.Fatal(new Exception("recovery exception")))
        }

      failure.failed.get.exception.getMessage shouldBe "recovery exception"
    }

    "recoverToAsync" in {
      Base.busyErrors() foreach {
        busy =>
          val failure =
            IO.Failure(busy) recoverToAsync {
              IO.Failure(busy) recoverToAsync {
                IO.Failure(busy) recoverToAsync {
                  IO.Failure(busy) recoverToAsync {
                    IO.Failure(busy) recoverToAsync {
                      IO.Failure(busy) recoverToAsync {
                        IO.Success(100)
                      }
                    }
                  }
                }
              }
            }
          failure.safeGetBlocking shouldBe IO.Success(100)
      }
    }

    "invoke onCompleteSideEffect" in {
      var invoked = false

      IO
        .Failure(new Exception("Oh no!"))
        .onCompleteSideEffect(_ => invoked = true)

      invoked shouldBe true
    }
  }
}

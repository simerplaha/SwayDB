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
import swaydb.ErrorHandler.Throwable
import swaydb.IO
import swaydb.data.Base._
import swaydb.data.io.Core
import swaydb.Error.Segment.ErrorHandler

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

class IOFailureSpec extends WordSpec with Matchers {

  "IO.Failure" should {
    "set boolean" in {
      val io = IO.Failure(swaydb.Error.OpeningFile(Paths.get(""), Reserve()))
      io.isFailure shouldBe true
      io.isDeferred shouldBe false
      io.isSuccess shouldBe false
    }

    "get" in {
      val io = IO.failed[Throwable, Unit](new IllegalAccessError)

      assertThrows[IllegalAccessError] {
        io.get
      }
      io.run shouldBe io
      io.runBlocking shouldBe io
      Try(io.runInFuture.await).failed.get.getCause shouldBe a[IllegalAccessError]
    }

    "getOrElse & orElse return first io if both are Failures" in {
      val io1 = IO.failed[Throwable, Unit](new IllegalAccessError)
      val io2 = IO.failed[Throwable, Unit](new IllegalArgumentException)

      (io1 getOrElse io2) shouldBe a[IO.Failure[IllegalArgumentException, Unit]]

      io1 orElse io2 shouldBe io2
    }

    "flatMap on Success" in {
      val failIO = IO.failed[Throwable, Int](new IllegalThreadStateException)
      failIO.asDeferred flatMap {
        i =>
          IO.Success[Throwable, Int](1)
      } shouldBe failIO
    }

    "flatMap on failure" in {
      val failure = IO.Failure(swaydb.Error.NoSuchFile(new NoSuchFileException("")))

      failure.asDeferred flatMap {
        _ =>
          IO.failed[swaydb.Error.Segment, Unit](new IllegalThreadStateException)
      } shouldBe failure
    }

    "flatten on failure with success" in {
      val io =
        IO.Failure[swaydb.Error.Segment, Int](swaydb.Error.Fatal(new Exception("Kaboom!"))).asIO map {
          _ =>
            IO.Success[swaydb.Error.Segment, Unit](11)
        }

      io.flatten.asInstanceOf[IO.Failure[Throwable, Int]].exception.getMessage shouldBe "Kaboom!"
    }

    "recover" in {
      val failure =
        IO.Failure(swaydb.Error.NoSuchFile(new NoSuchFileException(""))) recover {
          case _: swaydb.Error =>
            1
        }

      failure shouldBe IO.Success[swaydb.Error.Segment, Int](1)
    }

    "recoverWith" in {
      val failure =
        IO.Failure(swaydb.Error.NoSuchFile(new NoSuchFileException("")))
          .recoverWith[swaydb.Error.Segment, Unit] {
            case error: swaydb.Error.Segment =>
              IO.Failure(swaydb.Error.Fatal(new Exception("recovery exception")))
          }

      failure.failed.get.exception.getMessage shouldBe "recovery exception"
    }

    "recoverToAsync" in {
      Base.busyErrors() foreach {
        busy =>
          val failure =
            IO.Failure(busy) recoverToDeferred {
              IO.Failure(busy) recoverToDeferred {
                IO.Failure(busy) recoverToDeferred {
                  IO.Failure(busy) recoverToDeferred {
                    IO.Failure(busy) recoverToDeferred {
                      IO.Failure(busy) recoverToDeferred {
                        IO.Success[swaydb.Error.Segment, Int](100)
                      }
                    }
                  }
                }
              }
            }
          failure.runBlocking shouldBe IO.Success[swaydb.Error.Segment, Int](100)
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

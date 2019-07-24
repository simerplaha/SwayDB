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
import swaydb.data.io.Core
import swaydb.data.io.Core.Error.ErrorHandler

import scala.concurrent.ExecutionContext.Implicits.global

class IOSuccessSpec extends WordSpec with Matchers {
  "IO.Success" should {
    "set booleans" in {
      val io = IO.Success(1)
      io.isFailure shouldBe false
      io.isDeferred shouldBe false
      io.isSuccess shouldBe true
    }

    "get" in {
      val io = IO.Success(1)

      io.get shouldBe 1
      io.run shouldBe io
      io.runBlocking shouldBe io
      io.runInFuture.await shouldBe 1
    }

    "getOrElse & orElse return first io if both are successes" in {
      val io1 = IO.Success(1)
      val io2 = IO.Success(2)

      io1 getOrElse io2 shouldBe 1

      io1 orElse io2 shouldBe io1
    }

    "getOrElse return second io first is failure" in {
      val io = IO.Failure(Core.Error.NoSuchFile(new NoSuchFileException("")))

      io getOrElse 2 shouldBe 2

      io orElse IO.Success(2) shouldBe IO.Success(2)
    }

    "flatMap on Success" in {
      IO.Success(1).asIO flatMap {
        i =>
          IO.Success(i + 1)
      } shouldBe IO.Success(2)
    }

    "flatMap on failure" in {
      val failure = IO.Failure(Core.Error.NoSuchFile(new NoSuchFileException("")))

      IO.Success(1).asIO flatMap {
        _ =>
          failure
      } shouldBe failure
    }

    "flatMap on Async should return Async" in {
      val async =
        IO.Success(1).asDeferred flatMap {
          int =>
            IO.Defer(int + 1, Core.Error.OpeningFile(Paths.get(""), Reserve()))
        }

      async.get shouldBe 2
    }

    "flatten" in {
      val nested: IO[Throwable, IO[Throwable, IO[Throwable, IO[Throwable, Int]]]] = IO.Success(IO.Success(IO.Success(IO.Success(1))))
      nested.flatten.flatten.flatten shouldBe IO.Success(1)
    }

    "flatten on successes with failure" in {
      val nested: IO[Core.Error, IO[Core.Error, IO[Core.Error, IO[Core.Error, Int]]]] = IO.Success(IO.Success(IO.Success(IO.Failure(Core.Error.Fatal(new Exception("Kaboom!"))))))

      nested.flatten.flatten.flatten.asInstanceOf[IO.Failure[Core.Error, Int]].failed.get.exception.getMessage shouldBe "Kaboom!"
    }

    "invoke onCompleteSideEffect" in {
      var invoked = false

      IO
        .Success(123)
        .onCompleteSideEffect(_ => invoked = true)

      invoked shouldBe true
    }
  }
}

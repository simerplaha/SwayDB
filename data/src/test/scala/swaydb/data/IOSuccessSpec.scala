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

package swaydb.data

import java.nio.file.{NoSuchFileException, Paths}
import org.scalatest.{Matchers, WordSpec}
import scala.concurrent.ExecutionContext.Implicits.global
import swaydb.data.Base._

class IOSuccessSpec extends WordSpec with Matchers {
  "IO.Success" should {
    "set booleans" in {
      val io = IO.Success(1)
      io.isFailure shouldBe false
      io.isLater shouldBe false
      io.isSuccess shouldBe true
    }

    "get" in {
      val io = IO.Success(1)

      io.get shouldBe 1
      io.safeGet shouldBe io
      io.safeGetBlocking shouldBe io
      io.safeGetFuture.await shouldBe 1
    }

    "getOrElse & orElse return first io if both are successes" in {
      val io1 = IO.Success(1)
      val io2 = IO.Success(2)

      io1 getOrElse io2 shouldBe 1

      io1 orElse io2 shouldBe io1
    }

    "getOrElse return second io first is failure" in {
      val io = IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException("")))

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
      val failure = IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException("")))

      IO.Success(1).asIO flatMap {
        _ =>
          failure
      } shouldBe failure
    }

    "flatMap on Async should return Async" in {
      val async =
        IO.Success(1).asAsync flatMap {
          int =>
            IO.Async(int + 1, IO.Error.OpeningFile(Paths.get(""), Reserve()))
        }

      async.get shouldBe 2
    }

    "flatten" in {
      val nested: IO[IO[IO[IO[Int]]]] = IO.Success(IO.Success(IO.Success(IO.Success(1))))

      nested.flatten.flatten.flatten shouldBe IO.Success(1)
    }

    "flatten on successes with failure" in {
      val nested: IO[IO[IO[IO[Int]]]] = IO.Success(IO.Success(IO.Success(IO.Failure(IO.Error.Fatal(new Exception("Kaboom!"))))))

      nested.flatten.flatten.flatten.asInstanceOf[IO.Failure[Int]].exception.getMessage shouldBe "Kaboom!"
    }
  }
}

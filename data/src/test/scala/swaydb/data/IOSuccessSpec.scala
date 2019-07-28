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

import org.scalatest.{Matchers, WordSpec}
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.data.Base._

import scala.util.Try

class IOSuccessSpec extends WordSpec with Matchers {

  val error = swaydb.Error.Unknown(this.getClass.getSimpleName + " test exception.")

  "set booleans" in {
    val io = IO.Success(1)
    io.isFailure shouldBe false
    io.isSuccess shouldBe true
  }

  "get" in {
    IO.Success(1).get shouldBe 1
    IO.Success(2).get shouldBe 2
  }

  "exists" in {
    IO.Success(1).exists(_ == 1) shouldBe true
    IO.Success(1).exists(_ == 2) shouldBe false
  }

  "getOrElse" in {
    IO.Success(1) getOrElse 2 shouldBe 1
    IO.Failure(swaydb.Error.Unknown("")) getOrElse 3 shouldBe 3
  }

  "orElse" in {
    IO.Success(1) orElse IO.Success(2) shouldBe IO.Success(1)
    IO.Failure(error) orElse IO.Success(3) shouldBe IO.Success(3)
  }

  "foreach" in {
    IO.Success(1) foreach (_ shouldBe 1) shouldBe()
    IO.Success(2) foreach {
      i =>
        i shouldBe 2
        IO.Failure(error)
    } shouldBe()
  }

  "map" in {
    IO.Success(1) map {
      i =>
        i shouldBe 1
        i + 1
    } shouldBe IO.Success(2)
  }

  "map throws exception" in {
    IO.Success(1) map {
      i =>
        i shouldBe 1
        throw error.exception
    } shouldBe IO.Failure(error)
  }

  "flatMap on Success" in {
    IO.Success(1) flatMap {
      i =>
        i shouldBe 1
        IO.Success(i + 1)
    } shouldBe IO.Success(2)

    IO.Success(1) flatMap {
      i =>
        i shouldBe 1
        IO.Success(i + 1) flatMap {
          two =>
            two shouldBe 2
            IO.Success(two + 1)
        }
    } shouldBe IO.Success(3)
  }

  "flatMap on Failure" in {
    IO.Success(1) flatMap {
      i =>
        i shouldBe 1
        IO.Failure(error)
    } shouldBe IO.Failure(error)

    IO.Success(1) flatMap {
      i =>
        i shouldBe 1
        IO.Success(i + 1) flatMap {
          two =>
            IO.Failure(error)
        }
    } shouldBe IO.Failure(error)
  }

  "flatten" in {
    val nested: IO[Throwable, IO[Throwable, IO[Throwable, IO[Throwable, Int]]]] = IO.Success(IO.Success(IO.Success(IO.Success(1))))
    nested.flatten.flatten.flatten shouldBe IO.Success(1)
  }

  "flatten on successes with failure" in {
    val nested = IO.Success(IO.Success(IO.Success(IO.Failure(swaydb.Error.Unknown(new Exception("Kaboom!"))))))

    nested.flatten.flatten.flatten.failed.get.exception.getMessage shouldBe "Kaboom!"
  }

  "recover" in {
    IO.Success(1) recover {
      case _ =>
        fail("should not recover on success")
    } shouldBe IO.Success(1)
  }

  "recoverWith" in {
    IO.Success(2) recoverWith {
      case _ =>
        fail("should not recover on success")
    } shouldBe IO.Success(2)
  }

  "failed" in {
    IO.Success(2).failed shouldBe a[IO.Failure[_, _]]
  }

  "toOption" in {
    IO.Success(2).toOption shouldBe Some(2)
  }

  "toEither" in {
    IO.Success(2).toEither shouldBe Right(2)
  }

  "filter" in {
    IO.Success(2).filter(_ == 2) shouldBe IO(2)
    IO.Success(2).filter(_ == 1) shouldBe a[IO.Failure[_, _]]
  }

  "toFuture" in {
    val future = IO.Success(2).toFuture
    future.isCompleted shouldBe true
    future.await shouldBe 2
  }

  "toTry" in {
    IO.Success(2).toTry shouldBe Try(2)
  }

  "onFailureSideEffect" in {
    IO.Success(2) onFailureSideEffect {
      _ =>
        fail()
    } shouldBe IO.Success(2)
  }

  "onSuccessSideEffect" in {
    var invoked = false
    IO.Success(2) onSuccessSideEffect {
      _ =>
        invoked = true
    } shouldBe IO.Success(2)
    invoked shouldBe true
  }

  "onCompleteSideEffect" in {
    var invoked = false
    IO.Success(2) onCompleteSideEffect {
      _ =>
        invoked = true
    } shouldBe IO.Success(2)
    invoked shouldBe true
  }
}

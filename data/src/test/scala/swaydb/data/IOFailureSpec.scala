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

import java.io.FileNotFoundException

import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.data.Base._
import swaydb.Error.Segment.ErrorHandler

import scala.util.Try

class IOFailureSpec extends WordSpec with Matchers {

  val error = swaydb.Error.Unknown(this.getClass.getSimpleName + " test exception.")
  val otherError = swaydb.Error.FileNotFound(new FileNotFoundException())

  "set booleans" in {
    val io = IO.Failure(error)
    io.isFailure shouldBe true
    io.isSuccess shouldBe false
  }

  "get" in {
    assertThrows[Exception] {
      IO.Failure(error).get
    }
  }

  "exists" in {
    IO.Failure(error).exists(_ == 1) shouldBe false
  }

  "getOrElse" in {
    IO.Failure(error) getOrElse 2 shouldBe 2
    IO.Failure(swaydb.Error.Unknown("")) getOrElse 3 shouldBe 3
  }

  "orElse" in {
    IO.Failure(error) orElse IO.Success(2) shouldBe IO.Success(2)
    IO.Success(2) orElse IO.Failure(error) shouldBe IO.Success(2)
  }

  "foreach" in {
    IO.Failure(error) foreach (_ => fail()) shouldBe()
  }

  "map" in {
    IO.Failure(error) map {
      _ =>
        fail()
    } shouldBe IO.Failure(error)
  }

  "flatMap" in {
    IO.Failure(error) flatMap {
      i =>
        fail()
    } shouldBe IO.Failure(error)
  }

  "recover" in {
    IO.Failure(error) recover {
      case error =>
        error shouldBe this.error
        1
    } shouldBe IO.Success(1)
  }

  "recoverWith success" in {
    IO.Failure(error) recoverWith {
      case error =>
        error shouldBe this.error
        IO.Success(1)
    } shouldBe IO.Success(1)
  }

  "recoverWith failure" in {
    IO.Failure(error) recoverWith {
      case error =>
        error shouldBe this.error
        IO.Failure(otherError)
    } shouldBe IO.Failure(otherError)

    IO.Failure(error) recoverWith {
      case error =>
        error shouldBe this.error
        throw otherError.exception
    } shouldBe IO.Failure(otherError)
  }

  "failed" in {
    IO.Failure(error).failed.get shouldBe error
  }

  "toOption" in {
    IO.Failure(error).toOption shouldBe None
  }

  "toEither" in {
    IO.Failure(error).toEither shouldBe Left(error)
  }

  "filter" in {
    IO.Failure(error).filter(_ => fail()) shouldBe IO.Failure(error)
  }

  "toFuture" in {
    val future = IO.Failure(error).toFuture
    future.isCompleted shouldBe true
    assertThrows[Exception] {
      future.await
    }
  }

  "toTry" in {
    IO.Failure(error).toTry shouldBe scala.util.Failure(ErrorHandler.toException(error))
  }

  "onFailureSideEffect" in {
    var invoked = false
    IO.Failure(error) onFailureSideEffect {
      failure =>
        failure.error shouldBe this.error
        invoked = true
    } shouldBe IO.Failure(error)
    invoked shouldBe true
  }

  "onSuccessSideEffect" in {
    IO.Failure(error) onSuccessSideEffect {
      _ =>
        fail()
    } shouldBe IO.Failure(error)
  }

  "onCompleteSideEffect" in {
    var invoked = false
    IO.Failure(error) onCompleteSideEffect {
      io =>
        io shouldBe IO.Failure(error)
        invoked = true
    } shouldBe IO.Failure(error)
    invoked shouldBe true
  }
}

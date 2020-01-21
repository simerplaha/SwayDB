/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.data.Base._

class IOLeftSpec extends WordSpec with Matchers {

  val error = swaydb.Error.Fatal(this.getClass.getSimpleName + " test exception.")
  val otherError = swaydb.Error.FileNotFound(new FileNotFoundException())

  "set booleans" in {
    val io = IO.Left(error)
    io.isLeft shouldBe true
    io.isRight shouldBe false
  }

  "get" in {
    assertThrows[Exception] {
      IO.Left(error).get
    }
  }

  "exists" in {
    IO.Left(error).exists(_ == 1) shouldBe false
  }

  "getOrElse" in {
    IO.Left(error) getOrElse 2 shouldBe 2
    IO.Left(swaydb.Error.Fatal("")) getOrElse 3 shouldBe 3
  }

  "orElse" in {
    IO.Left(error) orElse IO.Right(2) shouldBe IO.Right(2)
    IO.Right(2) orElse IO.Left(error) shouldBe IO.Right(2)
  }

  "foreach" in {
    IO.Left(error) foreach (_ => fail()) shouldBe()
  }

  "map" in {
    IO.Left(error) map {
      _ =>
        fail()
    } shouldBe IO.Left(error)
  }

  "flatMap" in {
    IO.Left(error) flatMap {
      i =>
        fail()
    } shouldBe IO.Left(error)
  }

  "andThen" in {
    IO.Left(error) andThen {
      fail()
    } shouldBe IO.Left(error)
  }

  "and" in {
    IO.Left(error).and {
      fail()
    } shouldBe IO.Left(error)
  }

  "recover" in {
    IO.Left(error).recover {
      case error =>
        error shouldBe this.error
        1
    } shouldBe IO.Right(1)
  }

  "recoverWith success" in {
    IO.Left(error) recoverWith {
      case error =>
        error shouldBe this.error
        IO.Right(1)
    } shouldBe IO.Right(1)
  }

  "recoverWith failure" in {
    IO.Left(error: swaydb.Error.IO) recoverWith {
      case error =>
        error shouldBe this.error
        IO.Left(otherError: swaydb.Error.IO)
    } shouldBe IO.Left(otherError: swaydb.Error.IO)

    IO.Left(error: swaydb.Error.IO) recoverWith {
      case error =>
        error shouldBe this.error
        throw otherError.exception
    } shouldBe IO.Left(otherError: swaydb.Error.IO)
  }

  "failed" in {
    IO.Left(error).left.get shouldBe error
  }

  "toOption" in {
    IO.Left(error).toOption shouldBe None
  }

  "toEither" in {
    IO.Left(error).toEither shouldBe scala.util.Left(error)
  }

  "filter" in {
    IO.Left(error).filter(_ => fail()) shouldBe IO.Left(error)
  }

  "toFuture" in {
    val future = IO.Left(error).toFuture
    future.isCompleted shouldBe true
    assertThrows[Exception] {
      future.await
    }
  }

  "toTry" in {
    IO.Left(error).toTry shouldBe scala.util.Failure(IO.ExceptionHandler.toException(error))
  }

  "onFailureSideEffect" in {
    var invoked = false
    IO.Left(error) onLeftSideEffect {
      failure =>
        failure.value shouldBe this.error
        invoked = true
    } shouldBe IO.Left(error)
    invoked shouldBe true
  }

  "onSuccessSideEffect" in {
    IO.Left(error) onRightSideEffect {
      _ =>
        fail()
    } shouldBe IO.Left(error)
  }

  "onCompleteSideEffect" in {
    var invoked = false
    IO.Left(error) onCompleteSideEffect {
      io =>
        io shouldBe IO.Left(error)
        invoked = true
    } shouldBe IO.Left(error)
    invoked shouldBe true
  }
}

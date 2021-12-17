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

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.testkit.RunThis._

import java.io.FileNotFoundException

class IOLeftSpec extends AnyWordSpec {

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

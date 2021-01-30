/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.effect

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.testkit.RunThis._

import scala.util.Try

class IORightSpec extends AnyWordSpec with Matchers {

  val error = swaydb.Error.Fatal(this.getClass.getSimpleName + " test exception.")

  "set booleans" in {
    val io = IO.Right(1)
    io.isLeft shouldBe false
    io.isRight shouldBe true
  }

  "get" in {
    IO.Right(1).get shouldBe 1
    IO.Right(2).get shouldBe 2
  }

  "exists" in {
    IO.Right(1).exists(_ == 1) shouldBe true
    IO.Right(1).exists(_ == 2) shouldBe false
  }

  "getOrElse" in {
    IO.Right(1) getOrElse 2 shouldBe 1
    IO.Left(swaydb.Error.Fatal("")) getOrElse 3 shouldBe 3
  }

  "orElse" in {
    IO.Right(1) orElse IO.Right(2) shouldBe IO.Right(1)
    IO.Left(error) orElse IO.Right(3) shouldBe IO.Right(3)
  }

  "foreach" in {
    IO.Right(1) foreach (_ shouldBe 1) shouldBe()
    IO.Right(2) foreach {
      i =>
        i shouldBe 2
        IO.Left(error)
    } shouldBe()
  }

  "map" in {
    IO.Right(1) map {
      i =>
        i shouldBe 1
        i + 1
    } shouldBe IO.Right(2)
  }

  "map throws exception" in {
    IO.Right(1) map {
      i =>
        i shouldBe 1
        throw error.exception
    } shouldBe IO.Left(error)
  }

  "flatMap on Success" in {
    IO.Right(1) flatMap {
      i =>
        i shouldBe 1
        IO.Right(i + 1)
    } shouldBe IO.Right(2)

    IO.Right(1) flatMap {
      i =>
        i shouldBe 1
        IO.Right(i + 1) flatMap {
          two =>
            two shouldBe 2
            IO.Right(two + 1)
        }
    } shouldBe IO.Right(3)
  }

  "flatMap on Failure" in {
    IO.Right(1) flatMap {
      i =>
        i shouldBe 1
        IO.Left(error)
    } shouldBe IO.Left(error)

    IO.Right(1) flatMap {
      i =>
        i shouldBe 1
        IO.Right(i + 1) flatMap {
          two =>
            IO.Left(error)
        }
    } shouldBe IO.Left(error)
  }

  "andThen" in {
    IO.Right(1).andThen(2) shouldBe IO.Right(2)
  }

  "andThen throws exception" in {
    IO.Right(1).andThen(throw error.exception) shouldBe IO.Left(error)
  }

  "and on Success" in {
    IO.Right(1) and {
      IO.Right(2)
    } shouldBe IO.Right(2)

    IO.Right(1) and {
      IO.Right(2) flatMap {
        two =>
          two shouldBe 2
          IO.Right(two + 1)
      }
    } shouldBe IO.Right(3)
  }

  "and on Failure" in {
    IO.Right(1) and {
      IO.Left(error)
    } shouldBe IO.Left(error)

    IO.Right(1) and {
      IO.Right(2) and {
        IO.Left(error)
      }
    } shouldBe IO.Left(error)
  }

  "flatten" in {

    val nested = IO.Right(IO.Right(IO.Right(IO.Right(1))))
    nested.flatten.flatten.flatten shouldBe IO.Right(1)
  }

  "flatten on successes with failure" in {
    val nested = IO.Right(IO.Right(IO.Right(IO.Left(swaydb.Error.Fatal(new Exception("Kaboom!"))))))

    nested.flatten.flatten.flatten.left.get.exception.getMessage shouldBe "Kaboom!"
  }

  "recover" in {
    IO.Right(1) recover {
      case _ =>
        fail("should not recover on success")
    } shouldBe IO.Right(1)
  }

  "recoverWith" in {
    IO.Right(2) recoverWith {
      case _ =>
        fail("should not recover on success")
    } shouldBe IO.Right(2)
  }

  "failed" in {
    IO.Right(2).left shouldBe a[IO.Left[_, _]]
  }

  "toOption" in {
    IO.Right(2).toOption shouldBe Some(2)
  }

  "toEither" in {
    IO.Right(2).toEither shouldBe scala.util.Right(2)
  }

  "filter" in {
    IO.Right(2).filter(_ == 2) shouldBe IO(2)
    IO.Right(2).filter(_ == 1) shouldBe a[IO.Left[_, _]]
  }

  "toFuture" in {
    val future = IO.Right(2).toFuture
    future.isCompleted shouldBe true
    future.await shouldBe 2
  }

  "toTry" in {
    IO.Right(2).toTry shouldBe Try(2)
  }

  "onFailureSideEffect" in {
    IO.Right(2) onLeftSideEffect {
      _ =>
        fail()
    } shouldBe IO.Right(2)
  }

  "onSuccessSideEffect" in {
    var invoked = false
    IO.Right(2) onRightSideEffect {
      _ =>
        invoked = true
    } shouldBe IO.Right(2)
    invoked shouldBe true
  }

  "onCompleteSideEffect" in {
    var invoked = false
    IO.Right(2) onCompleteSideEffect {
      _ =>
        invoked = true
    } shouldBe IO.Right(2)
    invoked shouldBe true
  }
}

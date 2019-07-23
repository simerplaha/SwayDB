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
import swaydb.ErrorHandler.ThrowableErrorHandler
import swaydb.IO
import swaydb.IO._
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

class IOSpec extends WordSpec with Matchers {

  "CatchLeak" when {
    "exception" in {
      val exception = new Exception("Failed")
      IO.CatchLeak[IO.Error, Unit](throw exception).failed.get shouldBe IO.Error.Fatal(exception)
    }

    "no exception" in {
      IO.CatchLeak(IO.none).get shouldBe empty
    }
  }

  "foreachIO" should {
    "terminate in first failure returning the failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[IO.Failure[Throwable, Int]] =
        slice.foreachIO {
          item => {
            iterations += 1
            if (item == 3)
              IO.Failure(new Exception(s"result at item $item"))
            else
              IO.Success(item)
          }
        }

      result.isDefined shouldBe true
      result.get.exception.getMessage shouldBe "result at item 3"
      iterations shouldBe 3
    }

    "iterate all even on failure if failFast = false" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[IO.Failure[Throwable, Int]] =
        slice.foreachIO(
          item => {
            iterations += 1
            if (item == 3)
              IO.Failure(new Exception(s"result at item $item"))
            else {
              IO.Success(item)
            }
          },
          failFast = false
        )

      result.isDefined shouldBe true
      result.get.exception.getMessage shouldBe "result at item 3"
      iterations shouldBe 4
    }

    "finish iteration on IO.Success IO's" in {
      val slice = Slice(1, 2, 3, 4)

      val result: Option[IO.Failure[Throwable, Int]] =
        slice.foreachIO {
          item =>
            IO.Success(item)
        }

      result.isEmpty shouldBe true
    }
  }

  "mapIO" should {

    "allow map on Slice" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Throwable, Slice[Int]] =
        slice.mapIO {
          item =>
            IO.Success(item + 1)
        }

      result.get.toArray shouldBe Array(2, 3, 4, 5, 6)
    }

    "allow map on Slice, terminating at first failure returning the failure and cleanUp should be invoked" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val intsCleanedUp = ListBuffer.empty[Int]

      val result: IO[Throwable, Slice[Int]] =
        slice.mapIO(
          block = item => if (item == 3) IO.Failure(new Exception(s"Failed at $item")) else IO.Success(item),
          recover = (ints: Slice[Int], _: IO.Failure[Throwable, Slice[Int]]) => ints.foreach(intsCleanedUp += _)
        )

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at 3"

      intsCleanedUp should contain inOrderOnly(1, 2)
    }
  }

  "flatMapIO" should {

    "return flattened list" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Throwable, Iterable[String]] =
        slice flatMapIO {
          item =>
            IO(Seq(item.toString))
        }

      result.get.toArray shouldBe Array("1", "2", "3", "4", "5")
    }

    "return failure" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Throwable, Iterable[String]] =
        slice flatMapIO {
          item =>
            if (item < 3) {
              IO(List(item.toString))
            }
            else {
              IO.Failure(new Exception("Kaboom!"))
            }
        }
      result.failed.get.getMessage shouldBe "Kaboom!"
    }
  }

  "foldLeftIO" should {
    "allow fold, terminating at first failure returning the failure" in {
      val slice = Slice("one", "two", "three")

      val result: IO[Throwable, Int] =
        slice.foldLeftIO(0) {
          case (count, item) =>
            if (item == "two")
              IO.Failure(new Exception(s"Failed at $item"))
            else
              IO.Success(count + 1)
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at two"
    }

    "allow fold on Slice" in {
      val slice = Slice("one", "two", "three")

      val result: IO[Throwable, Int] =
        slice.foldLeftIO(0) {
          case (count, _) =>
            IO.Success(count + 1)
        }

      result.isSuccess shouldBe true
      result.get shouldBe 3
    }
  }

  "untilSome" should {
    "return first as success" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Throwable, Option[(Int, Int)]] =
        slice untilSome {
          item => {
            iterations += 1
            IO.Success(Some(item))
          }
        }

      result.get should contain((1, 1))
      iterations shouldBe 1
    }

    "return last as success" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Throwable, Option[(Int, Int)]] =
        slice untilSome {
          item => {
            iterations += 1
            if (item == 4)
              IO.Success(Some(item))
            else
              IO.none
          }
        }

      result.get should contain((4, 4))
      iterations shouldBe 4
    }

    "return mid result that resulted in Some value" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Throwable, Option[(Int, Int)]] =
        slice untilSome {
          item => {
            iterations += 1
            if (item == 3)
              IO.Success(Some(item))
            else
              IO.none
          }
        }

      result.get should contain((3, 3))
      iterations shouldBe 3
    }

    "return None if all iterations return None" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Throwable, Option[(Nothing, Int)]] =
        slice untilSome {
          _ => {
            iterations += 1
            IO.none
          }
        }

      result.get shouldBe empty
      iterations shouldBe 4
    }

    "terminate early on failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Throwable, Option[(Nothing, Int)]] =
        slice untilSome {
          item => {
            iterations += 1
            IO.Failure(new Exception(s"Failed at $item"))
          }
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at 1"
      iterations shouldBe 1
    }
  }

  "tryOrNone" when {
    "exception" in {
      IO.tryOrNone(throw new Exception("Failed")) shouldBe empty
    }

    "no exception" in {
      IO.tryOrNone("success").get shouldBe "success"
    }
  }

  "Failure fromTry" in {
    val exception = new Exception("failed")
    val failure = IO.fromTry(scala.util.Failure(exception))
    failure shouldBe a[IO.Failure[_, _]]
    failure.asInstanceOf[IO.Failure[_, _]].exception shouldBe exception
  }

  "Success fromTry" in {
    val failure = IO.fromTry(scala.util.Success(1))
    failure shouldBe a[IO.Success[_, _]]
    failure.asInstanceOf[IO.Success[_, _]].get shouldBe 1
  }
}

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

package swaydb.data.io

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.exceptions.TestFailedException
import scala.collection.mutable.ListBuffer
import swaydb.data.io.IO.orNone
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import IO._

class IOSpec extends WordSpec with Matchers with MockFactory {


  "Catch" when {
    "exception" in {
      val exception = new Exception("Failed")
      IO.Catch(throw exception).failed.get shouldBe exception
    }

    "no exception" in {
      IO.Catch(IO.successNone).get shouldBe empty
    }
  }

  "Slice.tryForeach" should {
    "terminate in first failure returning the failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[IO.Failure[Int]] =
        slice.foreachIO {
          item => {
            iterations += 1
            if (item == 3)
              IO.Failure(new Exception(s"result at item $item"))
            else
              IO.Sync(item)
          }
        }

      result.isDefined shouldBe true
      result.get.exception.getMessage shouldBe "result at item 3"
      iterations shouldBe 3
    }

    "iterate all even on failure if failFast = false" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[IO.Failure[Int]] =
        slice.foreachIO(
          item => {
            iterations += 1
            if (item == 3)
              IO.Failure(new Exception(s"result at item $item"))
            else {
              IO.Sync(item)
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

      val result: Option[IO.Failure[Int]] =
        slice.foreachIO {
          item =>
            IO.Sync(item)
        }

      result.isEmpty shouldBe true
    }
  }

  "Slice.tryMap" should {

    "allow map on Slice" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Slice[Int]] =
        slice.mapIO {
          item =>
            IO.Sync(item + 1)
        }

      result.get.toArray shouldBe Array(2, 3, 4, 5, 6)
    }

    "allow map on Slice, terminating at first failure returning the failure and cleanUp should be invoked" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val intsCleanedUp = ListBuffer.empty[Int]

      val result: IO[Slice[Int]] =
        slice.mapIO(
          ioBlock = item => if (item == 3) IO.Failure(new Exception(s"Failed at $item")) else IO.Sync(item),
          recover = (ints: Slice[Int], _: IO.Failure[Slice[Int]]) => ints.foreach(intsCleanedUp += _)
        )

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at 3"

      intsCleanedUp should contain inOrderOnly(1, 2)
    }
  }

  "Slice.tryFlattenIterable" should {

    "return flattened list" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Iterable[String]] =
        slice.flattenIterableIO {
          item =>
            IO(Seq(item.toString))
        }

      result.get.toArray shouldBe Array("1", "2", "3", "4", "5")
    }

    "return failure" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Iterable[String]] =
        slice.flattenIterableIO {
          item =>
            if (item < 3) {
              IO(Seq(item.toString))
            }
            else {
              IO.Failure(new Exception("Kaboom!"))
            }
        }
      result.failed.get.getMessage shouldBe "Kaboom!"
    }
  }

  "Slice.tryFoldLeft" should {
    "allow fold, terminating at first failure returning the failure" in {
      val slice = Slice("one", "two", "three")

      val result: IO[Int] =
        slice.foldLeftIO(0) {
          case (count, item) =>
            if (item == "two")
              IO.Failure(new Exception(s"Failed at $item"))
            else
              IO.Sync(count + 1)
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at two"
    }

    "allow fold on Slice" in {
      val slice = Slice("one", "two", "three")

      val result: IO[Int] =
        slice.foldLeftIO(0) {
          case (count, _) =>
            IO.Sync(count + 1)
        }

      result.isSuccess shouldBe true
      result.get shouldBe 3
    }
  }

  "Slice.toArray of a sub slice" should {
    "return the sub slice array only" in {
      val slice = Slice.create[Byte](ByteSizeOf.int * 3).addInt(123).addInt(456).addInt(789)
      slice.slice(0, 3).createReader().readInt() shouldBe 123
      slice.slice(4, 7).createReader().readInt() shouldBe 456
      slice.slice(8, 11).createReader().readInt() shouldBe 789
    }
  }

  "Slice.tryUntilSome" should {
    "return on first Successful result that resulted in Some value" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Option[(Int, Int)]] =
        slice.untilSome {
          item => {
            iterations += 1
            if (item == 3)
              IO.Sync(Some(item))
            else
              IO.successNone
          }
        }

      result.get shouldBe ((3, 3))
      iterations shouldBe 3
    }

    "return None if all iterations return None" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Option[(Int, Int)]] =
        slice.untilSome {
          _ => {
            iterations += 1
            IO.successNone
          }
        }

      result.get shouldBe empty
      iterations shouldBe 4
    }

    "terminate early on failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: IO[Option[(Int, Int)]] =
        slice.untilSome {
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
      orNone(throw new Exception("Failed")) shouldBe empty
    }

    "no exception" in {
      orNone("success").get shouldBe "success"
    }
  }


//  "Async" should {
//    "flatMap on IO.Success" in {
//      val io =
//        IO.Async(1).flatMapIO {
//          int =>
//            IO.Success(int + 1)
//        }
//
//      io.get shouldBe 2
//
//      io.getOrListen(_ => ???).get shouldBe 2
//    }
//
//    "flatMap on IO.Failure" in {
//      val io: IO.Async[Int] =
//        IO.Async(1).flatMapIO {
//          _ =>
//            IO.Failure(new TestFailedException("Kaboom!", 0))
//        }
//
//      assertThrows[TestFailedException] {
//        io.get
//      }
//
//      io.getOrListen(_ => ???) shouldBe a[IO.Failure[_]]
//    }
//
//    "getOrListen on IO.Failure" in {
//      var returnFailure = true
//
//      val io =
//        IO.Async(1).flatMapIO {
//          i =>
//            if (returnFailure)
//              IO.Failure(new TestFailedException("Kaboom!", 0))
//            else
//              IO.Success(i)
//        }
//
//      val listener = mockFunction[Int, Unit]
//      listener expects 1 returning()
//      io.getOrListen(listener) shouldBe a[IO.Failure[_]]
//      returnFailure = false
//      io.ready()
//    }
//  }
}

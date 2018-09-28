/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import swaydb.core.{FutureBase, TryAssert}
import swaydb.core.util.TryUtil._
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class TryUtilSpec extends WordSpec with Matchers with MockFactory with FutureBase with TryAssert {

  "runInFuture" should {
    "run the try in a new thread" in {
      def tryBlock: Try[Long] = Try(Thread.currentThread().getId)

      val tryBlockThreadId = tryBlock.tryInFuture.await(1.second)

      tryBlockThreadId should not be Thread.currentThread().getId
    }
  }

  "Slice.tryForeach" should {
    "terminate in first failure returning the failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[Failure[Int]] =
        slice.tryForeach {
          item => {
            iterations += 1
            if (item == 3)
              Failure(new Exception(s"result at item $item"))
            else
              Success(item)
          }
        }

      result.isDefined shouldBe true
      result.get.exception.getMessage shouldBe "result at item 3"
      iterations shouldBe 3
    }

    "iterate all even on failure if failFast = false" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Option[Failure[Int]] =
        slice.tryForeach(
          item => {
            iterations += 1
            if (item == 3)
              Failure(new Exception(s"result at item $item"))
            else {
              Success(item)
            }
          },
          failFast = false
        )

      result.isDefined shouldBe true
      result.get.exception.getMessage shouldBe "result at item 3"
      iterations shouldBe 4
    }

    "finish iteration on Success Try's" in {
      val slice = Slice(1, 2, 3, 4)

      val result: Option[Failure[Int]] =
        slice.tryForeach {
          item =>
            Success(item)
        }

      result.isEmpty shouldBe true
    }
  }

  "Slice.tryMap" should {

    "allow map on Slice" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: Try[Slice[Int]] =
        slice.tryMap {
          item =>
            Success(item + 1)
        }

      result.get.toArray shouldBe Array(2, 3, 4, 5, 6)
    }

    "allow map on Slice, terminating at first failure returning the failure and cleanUp should be invoked" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val intsCleanedUp = ListBuffer.empty[Int]

      val result: Try[Slice[Int]] =
        slice.tryMap(
          tryBlock = item => if (item == 3) Failure(new Exception(s"Failed at $item")) else Success(item),
          recover = (ints: Slice[Int], _: Failure[Slice[Int]]) => ints.foreach(intsCleanedUp += _)
        )

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at 3"

      intsCleanedUp should contain inOrderOnly(1, 2)
    }
  }

  "Slice.tryFlattenIterable" should {

    "return flattened list" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: Try[Iterable[String]] =
        slice.tryFlattenIterable {
          item =>
            Try(Seq(item.toString))
        }

      result.assertGet.toArray shouldBe Array("1", "2", "3", "4", "5")
    }

    "return failure" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: Try[Iterable[String]] =
        slice.tryFlattenIterable {
          item =>
            if (item < 3) {
              Try(Seq(item.toString))
            }
            else {
              Failure(new Exception("Kaboom!"))
            }
        }
      result.failed.assertGet.getMessage shouldBe "Kaboom!"
    }
  }

  "Slice.tryFoldLeft" should {
    "allow fold, terminating at first failure returning the failure" in {
      val slice = Slice("one", "two", "three")

      val result: Try[Int] =
        slice.tryFoldLeft(0) {
          case (count, item) =>
            if (item == "two")
              Failure(new Exception(s"Failed at $item"))
            else
              Success(count + 1)
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at two"
    }

    "allow fold on Slice" in {
      val slice = Slice("one", "two", "three")

      val result: Try[Int] =
        slice.tryFoldLeft(0) {
          case (count, _) =>
            Success(count + 1)
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

      val result: Try[Option[(Int, Int)]] =
        slice.tryUntilSome {
          item => {
            iterations += 1
            if (item == 3)
              Success(Some(item))
            else
              TryUtil.successNone
          }
        }

      result.assertGet shouldBe ((3, 3))
      iterations shouldBe 3
    }

    "return None if all iterations return None" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Try[Option[(Int, Int)]] =
        slice.tryUntilSome {
          _ => {
            iterations += 1
            TryUtil.successNone
          }
        }

      result.assertGetOpt shouldBe empty
      iterations shouldBe 4
    }

    "terminate early on failure" in {
      val slice = Slice(1, 2, 3, 4)
      var iterations = 0

      val result: Try[Option[(Int, Int)]] =
        slice.tryUntilSome {
          item => {
            iterations += 1
            Failure(new Exception(s"Failed at $item"))
          }
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at 1"
      iterations shouldBe 1
    }

  }
}

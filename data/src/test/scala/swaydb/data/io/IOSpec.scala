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

import java.util.concurrent.atomic.AtomicBoolean
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}
import scala.collection.mutable.ListBuffer
import swaydb.data.io.IO.{getOrNone, _}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

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

      val result: Option[IO.Failure[Int]] =
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

      val result: Option[IO.Failure[Int]] =
        slice.foreachIO {
          item =>
            IO.Success(item)
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
            IO.Success(item + 1)
        }

      result.get.toArray shouldBe Array(2, 3, 4, 5, 6)
    }

    "allow map on Slice, terminating at first failure returning the failure and cleanUp should be invoked" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val intsCleanedUp = ListBuffer.empty[Int]

      val result: IO[Slice[Int]] =
        slice.mapIO(
          ioBlock = item => if (item == 3) IO.Failure(new Exception(s"Failed at $item")) else IO.Success(item),
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
        slice.flattenIO {
          item =>
            IO(Seq(item.toString))
        }

      result.get.toArray shouldBe Array("1", "2", "3", "4", "5")
    }

    "return failure" in {
      val slice = Slice(1, 2, 3, 4, 5)

      val result: IO[Iterable[String]] =
        slice.flattenIO {
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
              IO.Success(count + 1)
        }

      result.isFailure shouldBe true
      result.failed.get.getMessage shouldBe "Failed at two"
    }

    "allow fold on Slice" in {
      val slice = Slice("one", "two", "three")

      val result: IO[Int] =
        slice.foldLeftIO(0) {
          case (count, _) =>
            IO.Success(count + 1)
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
              IO.Success(Some(item))
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
      getOrNone(throw new Exception("Failed")) shouldBe empty
    }

    "no exception" in {
      getOrNone("success").get shouldBe "success"
    }
  }

  "IO.Async" should {
    "flatMap on IO" in {
      val io =
        IO.Async(1, IO.Error.None) flatMap {
          int =>
            IO.Success(int + 1)
        }

      io.get shouldBe 2
    }

    "flatMap on IO.Failure" in {
      val io: IO.Async[Int] =
        IO.Async(1, IO.Error.None) flatMap {
          _ =>
            IO.Failure(IO.Error.OverlappingPushSegment)
        }

      assertThrows[Exception] {
        io.get
      }
    }

    "safeGet on multiple when last is a failure should return failure" in {
      val io: Async[Int] =
        IO.Async(1, IO.Error.None) flatMap {
          i =>
            println("1")
            IO.Async(i + 1, IO.Error.None) flatMap {
              i =>
                println("2")
                IO.Failure(IO.Error.FetchingValue(new AtomicBoolean(false)))
            }
        }

      io.safeGet shouldBe a[IO.Failure[_]]
    }

    "safeGet on multiple when last is Async should return last Async" in {
      val busy1 = new AtomicBoolean(true)
      val busy2 = new AtomicBoolean(true)
      val busy3 = new AtomicBoolean(true)

      val io: Async[Int] =
        IO.Async(1, IO.Error.DecompressingIndex(busy1)) flatMap {
          i =>
            IO.Async(i + 1, IO.Error.DecompressionValues(busy2)) flatMap {
              i =>
                IO.Async(i + 1, IO.Error.ReadingHeader(busy3))
            }
        }

      (1 to 100).par foreach {
        _ =>
          io.safeGet.asInstanceOf[IO.Later[_]].isValueDefined shouldBe false
          io.asInstanceOf[IO.Later[_]].isValueDefined shouldBe false
      }

      val io0 = io.safeGet
      io0 shouldBe io

      //make first IO available
      busy1.set(false)
      val io1 = io.safeGet
      io1 shouldBe a[IO.Async[_]]
      io0.safeGet shouldBe a[IO.Async[_]]

      //make second IO available
      busy2.set(false)
      val io2 = io.safeGet
      io2 shouldBe a[IO.Async[_]]
      io0.safeGet shouldBe a[IO.Async[_]]
      io1.safeGet shouldBe a[IO.Async[_]]

      //make third IO available. Now all IOs are ready, safeGet will result in Success.
      busy3.set(false)
      val io3 = io.safeGet
      io3 shouldBe IO.Success(3)
      io0.safeGet shouldBe IO.Success(3)
      io1.safeGet shouldBe IO.Success(3)
      io2.safeGet shouldBe IO.Success(3)

      //value should be defined on all instances.
      io0.asInstanceOf[IO.Later[_]].isValueDefined shouldBe true
      io1.asInstanceOf[IO.Later[_]].isValueDefined shouldBe true
      io2.asInstanceOf[IO.Later[_]].isValueDefined shouldBe true
    }

  }
}

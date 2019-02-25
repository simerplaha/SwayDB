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
import scala.concurrent.Future
import scala.util.Random
import swaydb.data.Base._

class IOAsyncSpec extends WordSpec with Matchers {

  "IO.Async" should {
    "flatMap on IO" in {
      val io =
        IO.Async(1, IO.Error.DecompressingValues(BusyBoolean(false))) flatMap {
          int =>
            IO.Success(int + 1)
        }

      io.get shouldBe 2
      io.safeGet shouldBe IO.Success(2)
      io.safeGetBlocking shouldBe IO.Success(2)
      io.safeGetFuture.await shouldBe IO.Success(2)
    }

    "flatMap on IO.Failure" in {
      val boolean = BusyBoolean(false)

      val io: IO.Async[Int] =
        IO.Async(1, IO.Error.DecompressingValues(BusyBoolean(false))) flatMap {
          _ =>
            IO.Failure(IO.Error.OpeningFile(Paths.get(""), boolean))
        }

      assertThrows[IO.Exception.OpeningFile] {
        io.get
      }

      io.safeGet.asInstanceOf[IO.Later[_]].error shouldBe IO.Error.OpeningFile(Paths.get(""), boolean)

    }

    "safeGet on multiple when last is a failure should return failure" in {
      val failure = IO.Failure(IO.Error.NoSuchFile(new NoSuchFileException("Not such file")))

      val io: IO.Async[Int] =
        IO.Async(1, IO.Error.DecompressingIndex(BusyBoolean(false))) flatMap {
          i =>
            IO.Async(i + 1, IO.Error.ReadingHeader(BusyBoolean(false))) flatMap {
              _ =>
                failure
            }
        }

      io.safeGet.asInstanceOf[IO.Later[_]].error shouldBe failure.error
    }

    "safeGet on multiple when last is Async should return last Async" in {
      val busy1 = BusyBoolean(true)
      val busy2 = BusyBoolean(true)
      val busy3 = BusyBoolean(true)

      val io: IO.Async[Int] =
        IO.Async(1, IO.Error.DecompressingIndex(busy1)) flatMap {
          i =>
            IO.Async(i + 1, IO.Error.DecompressingValues(busy2)) flatMap {
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
      BusyBoolean.setFree(busy1)
      val io1 = io.safeGet
      io1 shouldBe a[IO.Async[_]]
      io0.safeGet shouldBe a[IO.Async[_]]

      //make second IO available
      BusyBoolean.setFree(busy2)
      val io2 = io.safeGet
      io2 shouldBe a[IO.Async[_]]
      io0.safeGet shouldBe a[IO.Async[_]]
      io1.safeGet shouldBe a[IO.Async[_]]

      //make third IO available. Now all IOs are ready, safeGet will result in Success.
      BusyBoolean.setFree(busy3)
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

    "safeGetBlocking & safeGetFuture" in {
      import scala.concurrent.ExecutionContext.Implicits.global

      (1 to 2) foreach {
        i =>
          val io: IO.Async[Int] =
            (0 to 100).foldLeft(IO.Async(1, IO.Error.DecompressingIndex(BusyBoolean(false)))) {
              case (previous, i) =>
                previous flatMap {
                  output =>
                    val boolean = BusyBoolean(false)
                    Future {
                      if (Random.nextBoolean()) Thread.sleep(Random.nextInt(100))
                      BusyBoolean.setFree(boolean)
                    }
                    IO.Async(output + 1, Base.randomBusyException(boolean))
                }
            }

          if (i == 1)
            io.safeGetBlocking shouldBe IO.Success(102)
          else
            io.safeGetFuture.await shouldBe IO.Success(102)
      }

    }
  }
}

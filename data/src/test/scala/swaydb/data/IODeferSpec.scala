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

import java.nio.file.Paths

import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.data.Base._
import swaydb.data.io.Core

import scala.concurrent.ExecutionContext.Implicits.global

class IODeferSpec extends WordSpec with Matchers {

  "IO.Async" should {
    "flatMap on IO" in {
      val io =
        IO.Defer(1, Core.Error.DecompressingValues(Reserve())) flatMap {
          int =>
            IO.Success[Core.Error, Int](int + 1)
        }

      io.get shouldBe 2
      io.run shouldBe IO.Success(2)
      io.runBlocking shouldBe IO.Success(2)
      io.runInFuture.await shouldBe 2
    }

    "flatMap on IO.Failure" in {
      val boolean = Reserve(())

      val io =
        IO.Defer(1, Core.Error.DecompressingValues(Reserve())) flatMap {
          _ =>
            IO.Failure(Core.Error.OpeningFile(Paths.get(""), boolean))
        }

      assertThrows[Core.Exception.OpeningFile] {
        io.get
      }

      io.run.asInstanceOf[IO.Deferred[_, _]].error shouldBe Core.Error.OpeningFile(Paths.get(""), boolean)
    }
    //
    //    "safeGet on multiple when last is a failure should return failure" in {
    //      val failure = IO.Failure(Core.Error.NoSuchFile(new NoSuchFileException("Not such file")))
    //
    //      val io: IO.Defer[Int] =
    //        IO.Defer(1, Core.Error.DecompressingIndex(Reserve())) flatMap {
    //          i =>
    //            IO.Defer(i + 1, Core.Error.ReadingHeader(Reserve())) flatMap {
    //              _ =>
    //                failure
    //            }
    //        }
    //
    //      io.run.asInstanceOf[IO.Deferred[_]].error shouldBe failure.error
    //    }
    //
    //    "safeGet on multiple when last is Async should return last Async" in {
    //      val busy1 = Reserve(())
    //      val busy2 = Reserve(())
    //      val busy3 = Reserve(())
    //
    //      val io: IO.Defer[Int] =
    //        IO.Defer(1, Core.Error.DecompressingIndex(busy1)) flatMap {
    //          i =>
    //            IO.Defer(i + 1, Core.Error.DecompressingValues(busy2)) flatMap {
    //              i =>
    //                IO.Defer(i + 1, Core.Error.ReadingHeader(busy3))
    //            }
    //        }
    //
    //      (1 to 100).par foreach {
    //        _ =>
    //          io.run.asInstanceOf[IO.Deferred[_]].isValueDefined shouldBe false
    //          io.asInstanceOf[IO.Deferred[_]].isValueDefined shouldBe false
    //      }
    //
    //      val io0 = io.run
    //      io0 shouldBe io
    //
    //      //make first IO available
    //      Reserve.setFree(busy1)
    //      val io1 = io.run
    //      io1 shouldBe a[IO.Defer[_]]
    //      io0.run shouldBe a[IO.Defer[_]]
    //
    //      //make second IO available
    //      Reserve.setFree(busy2)
    //      val io2 = io.run
    //      io2 shouldBe a[IO.Defer[_]]
    //      io0.run shouldBe a[IO.Defer[_]]
    //      io1.run shouldBe a[IO.Defer[_]]
    //
    //      //make third IO available. Now all IOs are ready, safeGet will result in Success.
    //      Reserve.setFree(busy3)
    //      val io3 = io.run
    //      io3 shouldBe IO.Success(3)
    //      io0.run shouldBe IO.Success(3)
    //      io1.run shouldBe IO.Success(3)
    //      io2.run shouldBe IO.Success(3)
    //
    //      //value should be defined on all instances.
    //      io0.asInstanceOf[IO.Deferred[_]].isValueDefined shouldBe true
    //      io1.asInstanceOf[IO.Deferred[_]].isValueDefined shouldBe true
    //      io2.asInstanceOf[IO.Deferred[_]].isValueDefined shouldBe true
    //    }
    //
    //    "safeGetBlocking & safeGetFuture" in {
    //      import scala.concurrent.ExecutionContext.Implicits.global
    //
    //      (1 to 2) foreach {
    //        i =>
    //          val io: IO.Defer[Int] =
    //            (0 to 100).foldLeft(IO.Defer(1, Core.Error.DecompressingIndex(Reserve()))) {
    //              case (previous, i) =>
    //                previous flatMap {
    //                  output =>
    //                    val reserve = Reserve[Unit]()
    //                    Future {
    //                      if (Random.nextBoolean()) Thread.sleep(Random.nextInt(100))
    //                      Reserve.setFree(reserve)
    //                    }
    //                    IO.Defer(output + 1, Base.randomBusyException(reserve))
    //                }
    //            }
    //
    //          if (i == 1)
    //            io.runBlocking shouldBe IO.Success(102)
    //          else
    //            io.runInFuture.await shouldBe 102
    //      }
    //    }
    //
    //    "be initialised from Future" in {
    //      val result = IO.fromFuture(Future(1))
    //      result.runBlocking.get shouldBe 1
    //    }
    //
    //    "recover from Future failures" in {
    //      val failedMessage = "Something went wrong!"
    //
    //      (1 to 100) foreach {
    //        @volatile var failFuture = Random.nextBoolean()
    //
    //        def future: Future[Int] =
    //          if (!failFuture) {
    //            failFuture = true
    //            Future.failed(Base.randomBusyException().exception)
    //          } else {
    //            Future.failed(new Exception(failedMessage))
    //          }
    //
    //        _ =>
    //          val error =
    //            IO.fromFuture(future)
    //              .runBlocking
    //              .failed
    //              .get
    //
    //          //final error should always result is fatal because the above
    //          //future function will result failed message and will always
    //          //recover from busy errors.
    //          error shouldBe a[Core.Error.Fatal]
    //          error.exception.getMessage shouldBe failedMessage
    //      }
    //    }
    //  }
  }
}

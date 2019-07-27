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

import java.nio.file.{NoSuchFileException, Paths}

import org.scalatest.{Matchers, WordSpec}
import swaydb.IO
import swaydb.data.Base._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random
import IOValues._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration._

class IODeferSpec extends WordSpec with Matchers with Eventually {

  "it" should {
    "flatMap on IO" in {
      val io =
        IO.Defer(1, swaydb.Error.ReservedResource(Reserve())) flatMap {
          int =>
            IO.Success[swaydb.Error.Segment, Int](int + 1)
        }

      io.get shouldBe 2
      io.run shouldBe IO.Success(2)
      io.runBlocking shouldBe IO.Success(2)
      io.runInFuture.await shouldBe 2
    }

    "flatMap on IO.Failure" in {
      val boolean = Reserve(())

      val io =
        IO.Defer(1, swaydb.Error.ReservedResource(Reserve())) flatMap {
          _ =>
            IO.Failure(swaydb.Error.OpeningFile(Paths.get(""), boolean))
        }

      assertThrows[swaydb.Exception.OpeningFile] {
        io.get
      }

      io.run.asInstanceOf[IO.Deferred[_, _]].error shouldBe swaydb.Error.OpeningFile(Paths.get(""), boolean)
    }

    "safeGet on multiple when last is a failure should return failure" in {
      val failure = IO.Failure(swaydb.Error.NoSuchFile(new NoSuchFileException("Not such file")))

      val io: IO.Defer[swaydb.Error.Segment, Int] =
        IO.Defer(1, swaydb.Error.ReservedResource(Reserve())) flatMap {
          i =>
            IO.Defer(i + 1, swaydb.Error.ReservedResource(Reserve())) flatMap {
              _ =>
                failure
            }
        }

      io.run.asInstanceOf[IO.Deferred[_, _]].error shouldBe failure.error
    }

    "safeGet on multiple when last is Async should return last Async" in {
      val busy1 = Reserve(())
      val busy2 = Reserve(())
      val busy3 = Reserve(())

      val io: IO.Defer[swaydb.Error.Segment, Int] =
        IO.Defer(1, swaydb.Error.ReservedResource(busy1)) flatMap {
          i =>
            IO.Defer(i + 1, swaydb.Error.ReservedResource(busy2)) flatMap {
              i =>
                IO.Defer(i + 1, swaydb.Error.ReservedResource(busy3))
            }
        }

      (1 to 100).par foreach {
        _ =>
          io.run.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe false
          io.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe false
      }

      val io0 = io.run
      io0 shouldBe io

      //make first IO available
      Reserve.setFree(busy1)
      val io1 = io.run
      io1 shouldBe a[IO.Defer[_, _]]
      io0.run shouldBe a[IO.Defer[_, _]]

      //make second IO available
      Reserve.setFree(busy2)
      val io2 = io.run
      io2 shouldBe a[IO.Defer[_, _]]
      io0.run shouldBe a[IO.Defer[_, _]]
      io1.run shouldBe a[IO.Defer[_, _]]

      //make third IO available. Now all IOs are ready, safeGet will result in Success.
      Reserve.setFree(busy3)
      val io3 = io.run
      io3 shouldBe IO.Success(3)
      io0.run shouldBe IO.Success(3)
      io1.run shouldBe IO.Success(3)
      io2.run shouldBe IO.Success(3)

      //value should be defined on all instances.
      io0.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
      io1.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
      io2.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
    }

    "safeGetBlocking & safeGetFuture" in {
      import scala.concurrent.ExecutionContext.Implicits.global

      (1 to 10) foreach {
        i =>
          val io: IO.Defer[swaydb.Error.Segment, Int] =
            (0 to 100).foldLeft(IO.Defer[swaydb.Error.Segment, Int](1, swaydb.Error.ReservedResource(Reserve()))) {
              case (previous, _) =>
                previous flatMap {
                  output =>
                    val reserve = Reserve[Unit]()
                    Future {
                      if (Random.nextBoolean()) Thread.sleep(Random.nextInt(100))
                      Reserve.setFree(reserve)
                    }
                    IO.Defer(
                      value = output + 1,
                      error = Base.randomBusyError(reserve)
                    )
                }
            }

          if (i == 1)
            io.runBlocking shouldBe IO.Success(102)
          else
            io.runInFuture.await shouldBe 102
      }
    }

    "be initialised from Future" in {
      val result = IO.fromFuture(Future(1))
      result.runBlocking.get shouldBe 1
    }

    "recover from Future failures" in {
      val failedMessage = "Something went wrong!"

      @volatile var failFuture = Random.nextBoolean()

      def future: Future[Int] =
        Future {
          if (!failFuture)
            failFuture = true

          Thread.sleep(1.second.toMillis)
          throw new Exception(failedMessage)
        }

      //test that create a future does not block on the execution thread.
      eventually(Timeout(0.millisecond)) {
        IO.fromFuture[swaydb.Error.Segment, Int](future) shouldBe a[IO.Deferred[_, _]]
      }

      future.isCompleted shouldBe false
      val defer = IO.fromFuture[swaydb.Error.Segment, Int](future)
      future.isCompleted shouldBe false

      defer.failed.get shouldBe a[swaydb.Error.GetOnIncompleteDeferredFutureIO]

      Thread.sleep(2.seconds.toMillis)

      defer.failed.get.exception.getMessage shouldBe failedMessage
    }
  }
}

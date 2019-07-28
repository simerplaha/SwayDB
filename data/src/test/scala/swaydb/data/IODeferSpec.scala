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

import java.io.FileNotFoundException

import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpec}
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO.Deferred
import swaydb.{Error, IO}

import scala.util.Random

class IODeferSpec extends WordSpec with Matchers with Eventually with MockFactory {

  val error = swaydb.Error.Unknown(this.getClass.getSimpleName + " test exception.")
  val recoverableError = swaydb.Error.FileNotFound(new FileNotFoundException())

  "apply" in {
    //asserts that deferred operation does not get invoke on creating.
    IO.Deferred(fail())
    IO.Deferred(fail(), error)
  }

  "io" in {
    val deferred = IO.Deferred.io(IO(fail()))
    deferred.isReady shouldBe true
    deferred.isBusy shouldBe false
    deferred.isValueDefined shouldBe false
  }

  "future" in {
    fail()
  }

  "runIO" when {
    "successes" in {
      def doAssert[E](deferred: Deferred[E, Int]) = {
        deferred.isValueDefined shouldBe false
        deferred.isReady shouldBe true

        deferred.runIO.get shouldBe 1
        deferred.isValueDefined shouldBe true
        deferred.isReady shouldBe true
      }

      doAssert(IO.Deferred(1))
      doAssert(IO.Deferred(1, error))
      doAssert(IO.Deferred(() => 1, Some(error)))
      doAssert(IO.Deferred(() => 1, None))
    }

    "failures" in {
      def doAssert[E](deferred: Deferred[E, Int]) = {
        deferred.isValueDefined shouldBe false
        deferred.isReady shouldBe true

        deferred.runIO shouldBe IO.Failure(error)
        deferred.isValueDefined shouldBe false
        deferred.isReady shouldBe true
      }

      doAssert(IO.Deferred[swaydb.Error.Segment, Int](throw error.exception))
      doAssert(IO.Deferred(throw error.exception, error)) //is not reserved Error
      doAssert(IO.Deferred(throw error.exception, recoverableError))
      doAssert(IO.Deferred(if (Random.nextBoolean()) throw recoverableError.exception else throw error.exception, recoverableError))
      doAssert(IO.Deferred(() => throw error.exception, Some(error)))
    }
  }

  "map" when {
    "success" in {
      val mock = mockFunction[Int, Int]
      mock expects 1 returning 2

      val deferred =
        IO.Deferred(1) map {
          int =>
            int shouldBe 1
            mock(int)
        }
      deferred.isReady shouldBe true
      deferred.isValueDefined shouldBe false
      deferred.isBusy shouldBe false

      deferred.runIO shouldBe IO.Success(2)

      deferred.isReady shouldBe true
      deferred.isValueDefined shouldBe true
      deferred.isBusy shouldBe false

      //deferred's value is initialised initialised so the mock function is not invoked again.
      deferred.runIO shouldBe IO.Success(2)
    }
    "non-recoverable failure" in {
      var timesRun = 0

      val deferred =
        IO.Deferred[swaydb.Error.Segment, Int](1) map {
          int =>
            int shouldBe 1
            timesRun += 1
            throw error.exception
        }

      deferred.isReady shouldBe true
      deferred.isValueDefined shouldBe false
      deferred.isBusy shouldBe false

      deferred.runIO shouldBe IO.Failure(error)
      timesRun shouldBe 1
    }

    "recoverable failure" in {
      var timesRecovered = 0

      val deferred =
        IO.Deferred[swaydb.Error.Segment, Int](1) map {
          int =>
            int shouldBe 1
            //return recoverable errors 10 times and then non-recoverable errors.
            if (timesRecovered < 10) {
              timesRecovered += 1
              throw recoverableError.exception
            }
            else
              throw error.exception
        }

      deferred.isReady shouldBe true
      deferred.isValueDefined shouldBe false
      deferred.isBusy shouldBe false

      deferred.runIO shouldBe IO.Failure(error)
      timesRecovered shouldBe 10
    }
  }

  "flatMap" when {
    "successful" in {
      val mock1 = mockFunction[Int, Int]
      mock1 expects 1 returning 2

      val mock2 = mockFunction[Int, Int]
      mock2 expects 2 returning 3

      val mock3 = mockFunction[Int, Int]
      mock3 expects 3 returning 4

      val deferred =
        IO.Deferred(1) flatMap {
          int =>
            val nextInt = mock1(int)
            IO.Deferred(nextInt) flatMap {
              int =>
                val nextInt = mock2(int)
                IO.Deferred(nextInt) flatMap {
                  int =>
                    val nextInt = mock3(int)
                    IO.Deferred(nextInt)
                }
            }
        }

      deferred.isReady shouldBe true
      deferred.isBusy shouldBe false
      deferred.runIO shouldBe IO.Success(4)
      deferred.runIO shouldBe IO.Success(4)
    }

    "non-recoverable failure" in {
      val mock1 = mockFunction[Int]
      mock1 expects() returning 1

      val mock2 = mockFunction[Int, Int]
      mock2 expects 1 returning 2 repeat 1

      val mock3 = mockFunction[Int, Int]
      mock3 expects 2 returning 3 repeat 2

      val mock4 = mockFunction[Int, Int]
      mock4 expects 3 returning 4 repeat 2

      var secondCall = false

      val secondDeferredCached = IO.Deferred[swaydb.Error.Segment, Int](mock2(1))

      val deferred: Deferred[Error.Segment, Int] =
        IO.Deferred(mock1()) flatMap {
          int =>
            int shouldBe 1
            secondDeferredCached flatMap {
              int =>
                int shouldBe 2
                IO.Deferred[swaydb.Error.Segment, Int](mock3(int)) flatMap {
                  int =>
                    int shouldBe 3
                    IO.Deferred[swaydb.Error.Segment, Int](mock4(int)) flatMap {
                      int =>
                        int shouldBe 4
                        secondDeferredCached.isValueDefined shouldBe true
                        if (secondCall)
                          IO.Deferred(int + 1)
                        else
                          throw error.exception
                    }
                }
            }
        }

      deferred.isReady shouldBe true
      deferred.isBusy shouldBe false
      deferred.runIO shouldBe IO.Failure(error)
      secondCall = true
      deferred.runIO shouldBe IO.Success(5)
    }
  }

  //  "it" should {
  //    "flatMap on IO" in {
  //      val io =
  //        IO.Deferred(1, swaydb.Error.ReservedResource(Reserve())) flatMap {
  //          int =>
  //            IO.Success[swaydb.Error.Segment, Int](int + 1)
  //        }
  //
  //      io.get shouldBe 2
  //      io.run shouldBe IO.Success(2)
  //      io.runBlocking shouldBe IO.Success(2)
  //      io.runInFuture.await shouldBe 2
  //    }
  //
  //    "flatMap on IO.Failure" in {
  //      val boolean = Reserve(())
  //
  //      val io =
  //        IO.Deferred(1, swaydb.Error.ReservedResource(Reserve())) flatMap {
  //          _ =>
  //            IO.Failure(swaydb.Error.OpeningFile(Paths.get(""), boolean))
  //        }
  //
  //      assertThrows[swaydb.Exception.OpeningFile] {
  //        io.get
  //      }
  //
  //      io.run.asInstanceOf[IO.Deferred[_, _]].error shouldBe swaydb.Error.OpeningFile(Paths.get(""), boolean)
  //    }
  //
  //    "safeGet on multiple when last is a failure should return failure" in {
  //      val failure = IO.Failure(swaydb.Error.NoSuchFile(new NoSuchFileException("Not such file")))
  //
  //      val io: IO.Deferred[swaydb.Error.Segment, Int] =
  //        IO.Deferred(1, swaydb.Error.ReservedResource(Reserve())) flatMap  {
  //          i =>
  //            IO.Deferred(i + 1, swaydb.Error.ReservedResource(Reserve())) flatMap {
  //              _ =>
  //                failure
  //            }
  //        }
  //
  //      io.run.asInstanceOf[IO.Deferred[_, _]].error shouldBe failure.error
  //    }
  //
  //    "safeGet on multiple when last is Async should return last Async" in {
  //      val busy1 = Reserve(())
  //      val busy2 = Reserve(())
  //      val busy3 = Reserve(())
  //
  //      val io: IO.Deferred[swaydb.Error.Segment, Int] =
  //        IO.Deferred(1, swaydb.Error.ReservedResource(busy1)) flatMap {
  //          i =>
  //            IO.Deferred(i + 1, swaydb.Error.ReservedResource(busy2)) flatMap {
  //              i =>
  //                IO.Deferred(i + 1, swaydb.Error.ReservedResource(busy3))
  //            }
  //        }
  //
  //      (1 to 100).par foreach {
  //        _ =>
  //          io.run.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe false
  //          io.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe false
  //      }
  //
  //      val io0 = io.run
  //      io0 shouldBe io
  //
  //      //make first IO available
  //      Reserve.setFree(busy1)
  //      val io1 = io.run
  //      io1 shouldBe a[IO.Deferred[_, _]]
  //      io0.run shouldBe a[IO.Deferred[_, _]]
  //
  //      //make second IO available
  //      Reserve.setFree(busy2)
  //      val io2 = io.run
  //      io2 shouldBe a[IO.Deferred[_, _]]
  //      io0.run shouldBe a[IO.Deferred[_, _]]
  //      io1.run shouldBe a[IO.Deferred[_, _]]
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
  //      io0.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
  //      io1.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
  //      io2.asInstanceOf[IO.Deferred[_, _]].isValueDefined shouldBe true
  //    }
  //
  //    "safeGetBlocking & safeGetFuture" in {
  //      import scala.concurrent.ExecutionContext.Implicits.global
  //
  //      (1 to 10) foreach {
  //        i =>
  //          val io: IO.Deferred[swaydb.Error.Segment, Int] =
  //            (0 to 100).foldLeft(IO.Deferred[swaydb.Error.Segment, Int](1, swaydb.Error.ReservedResource(Reserve()))) {
  //              case (previous, _) =>
  //                previous flatMap {
  //                  output =>
  //                    val reserve = Reserve[Unit]()
  //                    Future {
  //                      if (Random.nextBoolean()) Thread.sleep(Random.nextInt(100))
  //                      Reserve.setFree(reserve)
  //                    }
  //                    IO.Deferred(
  //                      value = output + 1,
  //                      error = Base.randomBusyError(reserve)
  //                    )
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
  //      @volatile var failFuture = Random.nextBoolean()
  //
  //      def future: Future[Int] =
  //        Future {
  //          if (!failFuture)
  //            failFuture = true
  //
  //          Thread.sleep(3.second.toMillis)
  //          throw new Exception(failedMessage)
  //        }
  //
  //      //test that create a future does not block on the execution thread.
  //      eventually(Timeout(0.millisecond)) {
  //        IO.fromFuture[swaydb.Error.Segment, Int](future) shouldBe a[IO.Deferred[_, _]]
  //      }
  //
  //      future.isCompleted shouldBe false
  //      val defer = IO.fromFuture[swaydb.Error.Segment, Int](future)
  //      future.isCompleted shouldBe false
  //      defer.isCompleted shouldBe false
  //
  //      IO[swaydb.Error.Segment, Int](defer.get).failed.get shouldBe a[swaydb.Error.ReservedResource]
  //      defer.isCompleted shouldBe false
  //
  //      val ioError = swaydb.Error.FailedToWriteAllBytes(swaydb.Exception.FailedToWriteAllBytes(0, 0, 0))
  //
  //      val failureRecovery: IO.Deferred[Error.Segment, Int] =
  //        defer recoverWithDeferred {
  //          case error =>
  //            IO.Success[Error.IO, Int](1)
  //        }
  //
  //      defer.runBlocking.failed.get.exception.getMessage shouldBe failedMessage
  //      failureRecovery.runBlocking.failed.get shouldBe ioError
  //    }
  //  }
}

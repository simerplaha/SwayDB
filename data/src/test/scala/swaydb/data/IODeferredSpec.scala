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
import org.scalatest.concurrent.{Eventually, Futures}
import org.scalatest.{Matchers, WordSpec}
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO.Deferred
import swaydb.IOValues._
import swaydb.{Error, ErrorHandler, IO}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

class IODeferredSpec extends WordSpec with Matchers with Eventually with MockFactory with Futures {

  val unknownError = swaydb.Error.Fatal(this.getClass.getSimpleName + " test exception.")
  val recoverableError = swaydb.Error.FileNotFound(new FileNotFoundException())

  "apply" in {
    //asserts that deferred operation does not get invoke on creating.
    IO.Deferred(fail())
    IO.Deferred(fail(), unknownError)
  }

  "io" in {
    val deferred = IO.Deferred.io(IO(fail()))
    deferred.isReady shouldBe true
    deferred.isComplete shouldBe false
  }

  "future" when {
    def testFuture[E: ErrorHandler, A](future: Future[A], expectedOutcome: IO[E, A]) = {
      val timeBeforeDeferred = System.currentTimeMillis()

      future.isCompleted shouldBe false
      val defer = IO.fromFuture[swaydb.Error.Segment, A](future)
      future.isCompleted shouldBe false
      defer.isPending shouldBe true
      defer.isReady shouldBe true

      val timeAfterDeferred = System.currentTimeMillis()

      //creating a future should not block on executing thread.
      (timeAfterDeferred - timeBeforeDeferred) should be <= 200.millisecond.toMillis

      defer.runBlockingIO match {
        case IO.Success(value) =>
          value shouldBe expectedOutcome.get

        case IO.Failure(error) =>
          //on future failure the result Exception is wrapped within another Exception to stop recovery.
          error.exception.getCause shouldBe expectedOutcome.asInstanceOf[IO.Failure[E, A]].exception
      }
    }

    "failure" in {
      (1 to 5) foreach {
        _ =>
          //if the future returns a recoverable error it should still not perform recovery.
          //since the future is complete with failure and is not recoverable.
          val error = if (Random.nextBoolean()) recoverableError else unknownError

          def future: Future[Int] =
            Future {
              Thread.sleep(2.seconds.toMillis)
              throw error.exception
            }

          testFuture(future, IO.Failure(error))
      }
    }

    "success" in {
      (1 to 5) foreach {
        _ =>
          def future: Future[Int] =
            Future {
              Thread.sleep(2.seconds.toMillis)
              Int.MaxValue
            }

          testFuture(future, IO.Success(Int.MaxValue))
      }
    }

    "concurrent success" in {
      (1 to 5) foreach {
        _ =>
          val futures: Seq[Future[Int]] =
            (1 to 5) map {
              _ =>
                Future {
                  val sleeping = Random.nextInt(10)
                  println(s"Sleep for $sleeping.seconds")
                  Thread.sleep(sleeping.seconds.toMillis)
                  println(s"Completed sleep $sleeping")
                  1
                }
            }

          val defer1 = IO.fromFuture[Error.Segment, Int](futures(0))
          val defer2 = IO.fromFuture[Error.Segment, Int](futures(1))
          val defer3 = IO.fromFuture[Error.Segment, Int](futures(2))
          val defer4 = IO.fromFuture[Error.Segment, Int](futures(3))
          val defer5 = IO.fromFuture[Error.Segment, Int](futures(4))

          val createDefers = {
            defer1 flatMap {
              int1 =>
                defer2 flatMap {
                  int2 =>
                    defer3 flatMap {
                      int3 =>
                        defer4 flatMap {
                          int4 =>
                            defer5 map {
                              int5 =>
                                int1 + int2 + int3 + int4 + int5
                            }
                        }
                    }
                }
            }
          }
          if (Random.nextBoolean()) {
            createDefers.runIO shouldBe IO.Success(5)
            createDefers.runFutureIO shouldBe IO.Success(5)
          } else {
            createDefers.runFutureIO shouldBe IO.Success(5)
            createDefers.runIO shouldBe IO.Success(5)
          }
      }
    }
  }

  "runIO" when {
    "successes" in {
      def doAssert[E](deferred: Deferred[E, Int]) = {
        deferred.isComplete shouldBe false
        deferred.isReady shouldBe true

        deferred.runIO.get shouldBe 1
        deferred.isComplete shouldBe true
        deferred.isReady shouldBe true
      }

      doAssert(IO.Deferred(1))
      doAssert(IO.Deferred(1, unknownError))
      doAssert(IO.Deferred(() => 1, Some(unknownError)))
      doAssert(IO.Deferred(() => 1, None))
    }

    "failures" in {
      def doAssert[E](deferred: Deferred[E, Int]) = {
        deferred.isComplete shouldBe false
        deferred.isReady shouldBe true

        deferred.runIO shouldBe IO.Failure(unknownError)
        deferred.isComplete shouldBe false
        deferred.isReady shouldBe true
      }

      doAssert(IO.Deferred[swaydb.Error.Segment, Int](throw unknownError.exception))
      doAssert(IO.Deferred(throw unknownError.exception, unknownError)) //is not reserved Error
      doAssert(IO.Deferred(throw unknownError.exception, recoverableError))
      doAssert(IO.Deferred(if (Random.nextBoolean()) throw recoverableError.exception else throw unknownError.exception, recoverableError))
      doAssert(IO.Deferred(() => throw unknownError.exception, Some(unknownError)))
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
      deferred.isComplete shouldBe false

      deferred.runIO shouldBe IO.Success(2)

      deferred.isReady shouldBe true
      deferred.isComplete shouldBe true

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
            throw unknownError.exception
        }

      deferred.isReady shouldBe true
      deferred.isComplete shouldBe false

      deferred.runIO shouldBe IO.Failure(unknownError)
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
              throw unknownError.exception
        }

      deferred.isReady shouldBe true
      deferred.isComplete shouldBe false

      deferred.runIO shouldBe IO.Failure(unknownError)
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

      deferred.runIO shouldBe IO.Success(4)
      deferred.runIO shouldBe IO.Success(4)
    }

    "recoverable & non-recoverable failure" in {
      val value1 = mockFunction[Int]("value1")
      value1 expects() returning 1

      val value2 = mockFunction[Int, Int]("value2")
      value2 expects 1 returning 2

      val value3 = mockFunction[Int, Int]("value3")
      value3 expects 2 returning 3 repeat 4 //only expected this to be invoked multiple times since it's not cached.

      val value4 = mockFunction[Int, Int]("value4")
      value4 expects 3 returning 4

      //have 2 deferred as val so that their values get cached within.
      val secondDeferredCache = IO.Deferred[swaydb.Error.Segment, Int](value2(1))
      val fourthDeferredCache = IO.Deferred[swaydb.Error.Segment, Int](value4(3))
      //set the current error to throw.
      //the deferred tree below will set to be unknownError if a recoverable error is provided.
      var throwError: Option[Error] = Option(unknownError)

      val deferred: Deferred[Error.Segment, Int] =
        IO.Deferred(value1()) flatMap {
          int =>
            int shouldBe 1
            secondDeferredCache flatMap {
              int =>
                secondDeferredCache.isComplete shouldBe true
                int shouldBe 2
                IO.Deferred[swaydb.Error.Segment, Int](value3(int)) flatMap {
                  int =>
                    int shouldBe 3
                    fourthDeferredCache flatMap {
                      int =>
                        int shouldBe 4
                        fourthDeferredCache.isComplete shouldBe true
                        throwError map {
                          error =>
                            //if it's recoverable reset the error to be unknown so that call successfully succeeds.
                            //instead of running into an infinite loop.
                            if (error == recoverableError)
                              throwError = Some(unknownError)
                            throw error.exception
                        } getOrElse {
                          //if there is not error succeed.
                          IO.Deferred(int + 1)
                        }
                    }
                }
            }
        }

      deferred.isReady shouldBe true

      throwError = Some(unknownError)
      deferred.runBlockingIO shouldBe IO.Failure(unknownError)
      throwError = Some(recoverableError)
      //recoverableErrors are never returned
      deferred.runBlockingIO shouldBe IO.Failure(unknownError)
      throwError = None
      deferred.runBlockingIO shouldBe IO.Success(5)
    }
  }

  "flatMapIO" when {
    "successful deferred and IO" in {
      val deferred = IO.Deferred(10)

      deferred.isComplete shouldBe false
      deferred.isReady shouldBe true

      val ioDeferred: Deferred[Error.Segment, Int] =
        deferred flatMapIO {
          result =>
            result shouldBe 10
            IO.Success(result + 1)
        }

      ioDeferred.isComplete shouldBe false
      ioDeferred.isReady shouldBe true

      ioDeferred.runBlockingIO shouldBe IO.Success(11)
    }

    "successful deferred and failed IO" in {
      val deferred = IO.Deferred(10)

      deferred.isComplete shouldBe false
      deferred.isReady shouldBe true

      val failure = IO.failed[Error.Segment, Int]("Kaboom!")

      val ioDeferred: Deferred[Error.Segment, Int] =
        deferred flatMapIO {
          result =>
            result shouldBe 10
            failure
        }

      ioDeferred.isComplete shouldBe false
      ioDeferred.isReady shouldBe true

      ioDeferred.runBlockingIO shouldBe IO.Failure(swaydb.Error.Fatal(failure.exception))
    }

    "failed non-recoverable deferred and successful IO" in {
      val failure = IO.failed("Kaboom!")
      val deferred: Deferred[Error.Segment, Int] = IO.Deferred(throw failure.exception)

      deferred.isComplete shouldBe false
      deferred.isReady shouldBe true

      val ioDeferred: Deferred[Error.Segment, Int] =
        deferred flatMapIO {
          _ =>
            fail("should not have run")
        }

      ioDeferred.isComplete shouldBe false
      ioDeferred.isReady shouldBe true

      ioDeferred.runBlockingIO shouldBe IO.Failure(swaydb.Error.Fatal(failure.exception))
    }

    "failed recoverable deferred and successful IO" in {
      (1 to 100) foreach {
        _ =>
          var errorToUse = Option(recoverableError)

          val deferred: Deferred[Error.Segment, Int] =
            IO.Deferred {
              errorToUse map {
                error =>
                  //first time around throw the recoverable error and then no error.
                  errorToUse = None
                  throw error.exception
              } getOrElse {
                10
              }
            }

          deferred.isComplete shouldBe false
          deferred.isReady shouldBe true

          val ioDeferred: Deferred[Error.Segment, Int] =
            deferred flatMapIO {
              int =>
                int shouldBe 10
                IO.Success(int + 1)
            }

          ioDeferred.isComplete shouldBe false
          ioDeferred.isReady shouldBe true

          ioDeferred.runBlockingIO shouldBe IO.Success(11)
      }
    }
  }

  "recover" when {
    "non-recoverable failure" in {
      val deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    throw unknownError.exception
                }
            }
        } recover {
          case error: Error.Segment =>
            error shouldBe unknownError
            1
        }

      deferred.runIO shouldBe IO.Success(1)
    }

    "recoverable failure" in {
      @volatile var failureCount = 0

      def deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    if (failureCount >= 6) {
                      IO.Deferred(i + 1)
                    } else {
                      failureCount += 1
                      throw recoverableError.exception
                    }
                }
            }
        } recover {
          case _: Error.Segment =>
            fail("Didn't not expect recovery")
        }

      deferred.runBlockingIO shouldBe IO.Success(4)
      failureCount shouldBe 6
      failureCount = 0
      deferred.runFutureIO shouldBe IO.Success(4)
      failureCount shouldBe 6
    }

    "recoverable failure with non-recoverable failure result" in {
      @volatile var failureCount = 0

      def deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    if (failureCount >= 6) {
                      throw unknownError.exception
                    } else {
                      failureCount += 1
                      throw recoverableError.exception
                    }
                }
            }
        } recover {
          case error: Error.Segment =>
            error shouldBe unknownError
            Int.MaxValue
        }

      deferred.runBlockingIO shouldBe IO.Success(Int.MaxValue)
      failureCount shouldBe 6
      failureCount = 0
      deferred.runFutureIO shouldBe IO.Success(Int.MaxValue)
      failureCount shouldBe 6
    }
  }

  "recoverWith" when {
    "non-recoverable failure" in {
      def deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    throw unknownError.exception
                }
            }
        } recoverWith[Error.Segment, Int] {
          case error: Error.Segment =>
            error shouldBe unknownError
            IO.Deferred(1)
        }

      deferred.runBlockingIO shouldBe IO.Success(1)
      deferred.runFutureIO shouldBe IO.Success(1)
    }

    "non-recoverable failure when recoverWith result in recoverable Failure" in {

      @volatile var failureCount = 0

      def recoveredDeferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    if (failureCount >= 6) {
                      IO.Deferred(i + 1)
                    } else {
                      failureCount += 1
                      throw recoverableError.exception
                    }
                }
            }
        } recover {
          case _: Error.Segment =>
            fail("Didn't not expect recovery")
        }

      val deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    throw unknownError.exception
                }
            }
        } recoverWith[Error.Segment, Int] {
          case error: Error.Segment =>
            error shouldBe unknownError
            recoveredDeferred
        }

      deferred.runBlockingIO shouldBe IO.Success(4)
      deferred.runFutureIO shouldBe IO.Success(4)
    }

    "recoverable failure" in {
      @volatile var failureCount = 0

      def deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    if (failureCount >= 6) {
                      IO.Deferred(i + 1)
                    } else {
                      failureCount += 1
                      throw recoverableError.exception
                    }
                }
            }
        } recoverWith {
          case _: Error.Segment =>
            fail("Didn't not expect recovery")
        }

      deferred.runBlockingIO shouldBe IO.Success(4)
      failureCount shouldBe 6
      failureCount = 0
      deferred.runFutureIO shouldBe IO.Success(4)
      failureCount shouldBe 6
    }

    "recoverable failure with non-recoverable failure result" in {
      @volatile var failureCount = 0

      def deferred =
        IO.Deferred[Error.Segment, Int](1) flatMap {
          i =>
            IO.Deferred(i + 1) flatMap {
              i =>
                IO.Deferred(i + 1) flatMap {
                  i =>
                    if (failureCount >= 6) {
                      throw unknownError.exception
                    } else {
                      failureCount += 1
                      throw recoverableError.exception
                    }
                }
            }
        } recoverWith {
          case error: Error.Segment =>
            error shouldBe unknownError
            IO.Deferred(Int.MaxValue)
        }

      deferred.runBlockingIO shouldBe IO.Success(Int.MaxValue)
      failureCount shouldBe 6
      failureCount = 0
      deferred.runFutureIO shouldBe IO.Success(Int.MaxValue)
      failureCount shouldBe 6
    }
  }

  "concurrent randomly releases" in {
    val defers: Seq[IO.Deferred[Error.Segment, Int]] =
      (1 to 100) map {
        i =>
          if (Random.nextBoolean()) {
            var i = 0
            IO.Deferred[Error.Segment, Int] {
              if (i == 10) {
                1
              } else {
                i += 1
                throw recoverableError.exception
              }
            }
          } else if (Random.nextBoolean())
            IO.Deferred[Error.Segment, Int] {
              val sleeping = Random.nextInt(3)
              println(s"Sleep for $sleeping.seconds")
              Thread.sleep(sleeping.seconds.toMillis)
              1
            }
          else if(Random.nextBoolean())
            IO.Deferred[Error.Segment, Int] {
              if (Random.nextBoolean()) {
                1
              } else {
                throw recoverableError.exception
              }
            }
          else
            IO.Deferred(1)
      }

    val flattenedDefers =
      defers.foldLeft(IO.Deferred[Error.Segment, Int](1)) {
        case (previousDefer, nextDefer) =>
          previousDefer flatMap {
            _ =>
              nextDefer
          }
      }

    flattenedDefers.runIO.get shouldBe 1
  }
}

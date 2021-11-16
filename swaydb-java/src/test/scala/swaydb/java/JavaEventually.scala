/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.java

import swaydb.utils.FiniteDurations._

import scala.compat.java8.DurationConverters._
import scala.concurrent.TimeoutException
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.{Failure, Success, Try}

object JavaEventually {
  def sleep(time: FiniteDuration): Unit = {
    println(s"Sleeping: ${time.asString}")
    Thread.sleep(time.toMillis)
  }

  def sleep(time: java.time.Duration): Unit = {
    println(s"Sleeping: ${time.toScala.asString}")
    Thread.sleep(time.toMillis)
  }

  def eventually[T](test: Test): Unit =
    eventually(
      timeoutDuration = 2.seconds,
      interval = 0.5.seconds,
      test = test
    )

  def eventually[T](timeout: Int,
                    test: Test): Unit =

    eventually(
      timeoutDuration = timeout.seconds,
      interval = (timeout.seconds.toMillis / 5).millisecond,
      test = test
    )

  def eventually[T](timeout: FiniteDuration,
                    test: Test): Unit =

    eventually(
      timeoutDuration = timeout,
      interval = (timeout.toMillis / 5).millisecond,
      test = test
    )

  def eventually[T](timeout: Int,
                    interval: Int,
                    test: Test): Unit =
    eventually(
      timeoutDuration = timeout.seconds,
      interval = interval.seconds,
      test = test
    )

  def eventually[T](timeoutDuration: FiniteDuration,
                    interval: FiniteDuration,
                    test: Test): Unit = {
    val deadline = timeoutDuration.fromNow
    var keepTrying: Boolean = true
    var result: Option[Throwable] = Some(new TimeoutException("Test timed-out!"))

    while (keepTrying)
      Try(test.assert()) match {
        case Failure(exception) =>
          if (deadline.isOverdue()) {
            result = Some(exception)
            keepTrying = false
          } else {
            sleep(interval)
          }
        case Success(value) =>
          result = None
          keepTrying = false
      }

    result foreach {
      error =>
        throw error
    }
  }
}

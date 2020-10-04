/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.java

import swaydb.data.util.FiniteDurations.FiniteDurationImplicits

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

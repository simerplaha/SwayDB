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

package swaydb.data.java

import java.util.function.Supplier

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

trait JavaEventually {

  private def sleep(time: FiniteDuration): Unit =
    Thread.sleep(time.toMillis)

  def eventually[T](test: Supplier[T]): T =
    eventually(
      timeoutDuration = 2.seconds,
      interval = (2 / 5).seconds,
      test = test
    )

  def eventuallyInSeconds[T](timeout: Int,
                             test: Supplier[T]): T =
    eventually(
      timeoutDuration = timeout.seconds,
      interval = (timeout / 5).seconds,
      test = test
    )

  def eventuallyInSeconds[T](timeout: Int,
                             interval: Int,
                             test: Supplier[T]): T =
    eventually(
      timeoutDuration = timeout.seconds,
      interval = interval.seconds,
      test = test
    )

  def eventually[T](timeoutDuration: FiniteDuration,
                    interval: FiniteDuration,
                    test: Supplier[T]): T = {
    val deadline = timeoutDuration.fromNow
    var keepTrying: Boolean = true
    var result: Either[Throwable, T] = Left(new TimeoutException("Test timed-out!"))

    while (keepTrying)
      Try(test.get()) match {
        case Failure(exception) =>
          if (deadline.isOverdue()) {
            result = Left(exception)
            keepTrying = false
          } else {
            sleep(interval)
          }
        case Success(value) =>
          result = Right(value)
          keepTrying = false
      }

    result match {
      case Right(success) =>
        success

      case Left(failure) =>
        throw failure
    }
  }
}

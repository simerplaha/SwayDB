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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.data.util

import java.util.TimerTask
import java.util.concurrent.TimeUnit

import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

private[swaydb] object FiniteDurations {

  implicit class FiniteDurationImplicits(duration: Duration) {

    @inline final def asString: String = {
      val seconds: Double = duration.toMillis / 1000D
      val scaledSeconds = Maths.round(seconds)
      s"$scaledSeconds seconds"
    }
  }

  implicit class TimerTaskToDuration(task: TimerTask) {
    @inline final def deadline() =
      timeLeft().fromNow

    @inline final def timeLeft(): FiniteDuration =
      FiniteDuration(task.scheduledExecutionTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS)
  }

  /**
   * Key-values such as Groups and Ranges can contain deadlines internally.
   *
   * Groups's internal key-value can contain deadline and Range's from and range value contain deadline.
   * Be sure to extract those before checking for nearest deadline. Use other [[getNearestDeadline]]
   * functions instead that take key-value as input to fetch the correct nearest deadline.
   */
  def getNearestDeadline(deadline: Option[Deadline],
                         next: Option[Deadline]): Option[Deadline] =
    (deadline, next) match {
      case (Some(previous), Some(next)) =>
        if (previous < next)
          Some(previous)
        else
          Some(next)

      case (None, next @ Some(_)) =>
        next

      case (previous @ Some(_), None) =>
        previous

      case (None, None) =>
        None
    }

  def eventually[T](timeoutDuration: FiniteDuration = 1.seconds,
                    interval: FiniteDuration = 100.millisecond)(f: => T): Try[T] = {
    val deadline = timeoutDuration.fromNow
    var keepTrying: Boolean = true
    var result: Try[T] = Failure(new TimeoutException("Timeout!"))

    while (keepTrying)
      Try(f) match {
        case Failure(exception) =>
          if (deadline.isOverdue()) {
            result = Failure(exception)
            keepTrying = false
          } else {
            Thread.sleep(interval.toMillis)
          }
        case Success(value) =>
          result = Success(value)
          keepTrying = false
      }

    result
  }
}

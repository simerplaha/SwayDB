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

package swaydb.utils

import java.util.TimerTask
import java.util.concurrent.TimeUnit
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

private[swaydb] object FiniteDurations {

  implicit class FiniteDurationImplicits(duration: Duration) {

    @inline final def asString: String =
      asString(scale = 6)

    @inline final def asString(scale: Int): String = {
      val seconds: Double = duration.toMillis / 1000D
      val scaledSeconds = Maths.round(seconds, scale)
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

  def getFurthestDeadline(deadline: Option[Deadline],
                          next: Option[Deadline]): Option[Deadline] =
    (deadline, next) match {
      case (Some(previous), Some(next)) =>
        if (previous > next)
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

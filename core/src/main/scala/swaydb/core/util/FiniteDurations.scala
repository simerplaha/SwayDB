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

package swaydb.core.util

import java.util.TimerTask
import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

object FiniteDurations {

  implicit class FiniteDurationImplicits(duration: Duration) {
    def asString: String = {
      val seconds: Double = duration.toMillis / 1000D
      s"$seconds seconds"
    }
  }

  implicit class TimerTaskToDuration(task: TimerTask) {
    def deadline() =
      timeLeft().fromNow

    def timeLeft(): FiniteDuration =
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
      case (None, next @ Some(_)) => next
      case (previous @ Some(_), None) => previous
      case (None, None) => None
    }
}

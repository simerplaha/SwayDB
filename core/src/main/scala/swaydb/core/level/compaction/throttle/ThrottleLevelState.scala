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
 */

package swaydb.core.level.compaction.throttle

import swaydb.core.util.FiniteDurations._

import scala.concurrent.Promise
import scala.concurrent.duration.{Deadline, _}

private[level] sealed trait ThrottleLevelState {
  def stateId: Long
}

private[level] object ThrottleLevelState {
  val failureSleepDuration = 5.second

  def longSleep = longSleepFiniteDuration.fromNow

  val longSleepFiniteDuration: FiniteDuration = 1.hour

  case class AwaitingPull(promise: Promise[Unit],
                          timeout: Deadline,
                          stateId: Long) extends ThrottleLevelState {
    @volatile var listenerInvoked: Boolean = false
    @volatile var listenerInitialised: Boolean = false

    override def toString: String =
      this.getClass.getSimpleName +
        s"timeout: ${timeout.timeLeft.asString}, " +
        s"stateId: $stateId, " +
        s"listenerInvoked: $listenerInvoked, " +
        s"listenerInitialised: $listenerInitialised"
  }

  case class Sleeping(sleepDeadline: Deadline,
                      stateId: Long) extends ThrottleLevelState {
    override def toString: String =
      this.getClass.getSimpleName +
        s" - sleepDeadline: ${sleepDeadline.timeLeft.asString}, " +
        s"stateId: $stateId "
  }
}

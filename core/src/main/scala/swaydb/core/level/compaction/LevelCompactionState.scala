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

package swaydb.core.level.compaction

import swaydb.core.util.FiniteDurationUtil._

import swaydb.data.IO

import scala.concurrent.duration.{Deadline, _}

private[level] sealed trait LevelCompactionState {
  def previousStateID: Long
}
private[level] object LevelCompactionState {
  val longSleepDuration = 1.hour
  val failureSleepDuration = 5.second.fromNow
  def longSleepDeadline = longSleepDuration.fromNow

  case class AwaitingPull(later: IO.Later[_],
                          timeout: Deadline,
                          previousStateID: Long) extends LevelCompactionState {
    @volatile var isReady: Boolean = false
    @volatile var listenerInitialised: Boolean = false

    override def toString: String =
      this.getClass.getSimpleName +
        s" - later: ${later.getClass.getSimpleName}, " +
        s"timeout: ${timeout.timeLeft.asString}, " +
        s"previousStateID: $previousStateID"
  }

  case class Sleep(sleepDeadline: Deadline, previousStateID: Long) extends LevelCompactionState {
    override def toString: String =
      this.getClass.getSimpleName +
        s" - sleepDeadline: ${sleepDeadline.timeLeft.asString}, " +
        s"previousStateID: $previousStateID"
  }

  def longSleep(stateID: Long) = Sleep(longSleepDuration.fromNow, stateID)
}

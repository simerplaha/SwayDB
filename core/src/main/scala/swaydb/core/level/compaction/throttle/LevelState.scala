/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.throttle

import swaydb.utils.FiniteDurations.FiniteDurationImplicits

import scala.concurrent.Future
import scala.concurrent.duration._

private[level] object LevelState {

  val failureSleepDuration: FiniteDuration = 5.second

  def longSleep: Deadline = longSleepFiniteDuration.fromNow

  val longSleepFiniteDuration: FiniteDuration = 1.hour

  case class Sleeping(sleepDeadline: Deadline,
                      stateId: Long) {
    override def toString: String =
      this.productPrefix +
        s" - sleepDeadline: ${sleepDeadline.timeLeft.asString}, " +
        s"stateId: $stateId "

    def toFuture: Future[Sleeping] =
      Future.successful(this)
  }
}

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

package swaydb.core.level.compaction.throttle.behaviour

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorWire
import swaydb.core.level._
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.task.{CompactionLevelTasker, CompactionLevelZeroTasker, CompactionTask}
import swaydb.core.level.compaction.throttle.ThrottleCompactor.ForwardResponse
import swaydb.core.level.compaction.throttle.behaviour.ThrottleForwardBehavior.logger
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleCompactorState, ThrottleLevelState}
import swaydb.core.level.zero.LevelZero
import swaydb.data.NonEmptyList
import swaydb.data.slice.Slice
import swaydb.data.util.FiniteDurations
import swaydb.data.util.FiniteDurations.FiniteDurationImplicits
import swaydb.data.util.Futures._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Implements compaction functions.
 */
private[throttle] object ThrottleExtendBehavior extends LazyLogging {

  def extensionSuccessful(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                         locker: ActorWire[LastLevelLocker, Unit],
                                                         ec: ExecutionContext,
                                                         self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    ThrottleWakeUpBehavior.wakeUp(state)

  def extensionFailed(state: ThrottleCompactorState): Future[ThrottleCompactorState] = {
    logger.error("Failed to extend")
    Future.successful(state)
  }
}

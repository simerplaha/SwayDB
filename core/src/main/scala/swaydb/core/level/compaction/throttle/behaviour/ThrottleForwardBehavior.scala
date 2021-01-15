/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
import swaydb.DefActor
import swaydb.core.level.Level
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.throttle.ThrottleCompactor.ForwardResponse
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleCompactorState}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Implements compaction functions.
 */
private[throttle] object ThrottleForwardBehavior extends LazyLogging {

  def forward(level: Level,
              state: ThrottleCompactorState,
              replyTo: DefActor[ForwardResponse, Unit])(implicit committer: DefActor[CompactionCommitter.type, Unit],
                                                        locker: DefActor[LastLevelLocker, Unit],
                                                        ec: ExecutionContext,
                                                        self: DefActor[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    if (level.levelNumber + 1 != state.levels.head.levelNumber) {
      logger.error(s"${state.name}: Cannot forward Level. Forward Level(${level.levelNumber}) is not previous level of Level(${state.levels.head.levelNumber})")
      replyTo.send(_.forwardFailed(level))
      Future.successful(state)
    } else {
      ThrottleWakeUpBehavior
        .wakeUp(state.copy(levels = state.levels prepend level))
        .map {
          newState =>
            val updatedState =
              newState.copy(
                levels = state.levels,
                compactionStates = newState.compactionStates.filterNot(_._1.levelNumber == level.levelNumber)
              )

            replyTo.send(_.forwardSuccessful(level))
            updatedState
        }
        .recover {
          case throwable =>
            logger.error(s"${state.name}: Forward failed", throwable)
            replyTo.send(_.forwardFailed(level))
            state
        }
    }

  def forwardSuccessful(level: Level,
                        state: ThrottleCompactorState): Future[ThrottleCompactorState] =
    if (state.levels.last.levelNumber + 1 != level.levelNumber) {
      logger.error(s"${state.name}: Forward success update failed because Level(${state.levels.last.levelNumber}) + 1 != Leve(${level.levelNumber})")
      Future.successful(state)
    } else {
      Future.successful(state.copy(levels = state.levels append level))
    }

  def forwardFailed(level: Level,
                    state: ThrottleCompactorState): Future[ThrottleCompactorState] =
    if (state.levels.last.levelNumber + 1 != level.levelNumber) {
      logger.error(s"${state.name}: Forward success update failed because Level(${state.levels.last.levelNumber}) + 1 != Leve(${level.levelNumber})")
      Future.successful(state)
    } else {
      Future.successful(state.copy(levels = state.levels append level))
    }
}

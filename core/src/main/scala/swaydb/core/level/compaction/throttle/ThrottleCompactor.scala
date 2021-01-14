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

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorWire
import swaydb.core.level.Level
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.throttle.ThrottleCompactor.ForwardResponse
import swaydb.core.level.compaction.throttle.behaviour._

import scala.concurrent.Future

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */

object ThrottleCompactor {
  sealed trait ForwardResponse {
    def forwardSuccessful(level: Level): Unit

    def forwardFailed(level: Level): Unit
  }

  def apply(state: ThrottleCompactorState)(self: ActorWire[ThrottleCompactor, Unit])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                                              locker: ActorWire[LastLevelLocker, Unit]) =
    new ThrottleCompactor(state, Future.unit)(self, committer, locker)
}

private[core] class ThrottleCompactor private(@volatile private var state: ThrottleCompactorState,
                                              @volatile private var currentFuture: Future[Unit])(implicit self: ActorWire[ThrottleCompactor, Unit],
                                                                                                 committer: ActorWire[CompactionCommitter.type, Unit],
                                                                                                 locker: ActorWire[LastLevelLocker, Unit]) extends Compactor with ForwardResponse with LastLevelLocker.LastLevelSetResponse with LazyLogging {

  implicit val ec = self.ec

  @inline private def onComplete(f: => Future[ThrottleCompactorState]): Unit =
    currentFuture onComplete {
      _ =>
        this.currentFuture =
          f map {
            newState =>
              this.state = newState
          }
    }

  override def wakeUp(): Unit =
    onComplete(ThrottleWakeUpBehavior.wakeUp(state))

  def forward(level: Level, replyTo: ActorWire[ForwardResponse, Unit]): Unit =
    onComplete(ThrottleForwardBehavior.forward(level, state, replyTo))

  override def forwardSuccessful(level: Level): Unit =
    onComplete(ThrottleForwardBehavior.forwardSuccessful(level, state))

  override def forwardFailed(level: Level): Unit =
    onComplete(ThrottleForwardBehavior.forwardFailed(level, state))

  override def levelSetSuccessful(): Unit =
    onComplete(ThrottleExtendBehavior.extensionSuccessful(state))

  override def levelSetFailed(): Unit =
    onComplete(ThrottleExtendBehavior.extensionFailed(state))
}

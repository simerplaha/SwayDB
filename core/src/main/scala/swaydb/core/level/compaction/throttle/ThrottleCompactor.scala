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
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.throttle.ThrottleCompactor.PauseResponse

import scala.concurrent.{ExecutionContext, Future}

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */

object ThrottleCompactor {
  sealed trait PauseResponse {
    def paused(): Unit
  }

  def apply(state: ThrottleCompactorState.Compacting)(self: ActorWire[Compactor, Unit])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                                        locker: ActorWire[LastLevelLocker, Unit]) =
    new ThrottleCompactor(state, Future.unit)(self, committer, locker)
}

private[core] class ThrottleCompactor private(@volatile private var state: ThrottleCompactorState,
                                              @volatile private var currentFuture: Future[Unit])(implicit self: ActorWire[Compactor, Unit],
                                                                                                 committer: ActorWire[CompactionCommitter.type, Unit],
                                                                                                 locker: ActorWire[LastLevelLocker, Unit]) extends Compactor with PauseResponse with LastLevelLocker.ExtensionResponse with LazyLogging {

  implicit val ec = self.ec

  @inline private def executeSequential(f: => Future[ThrottleCompactorState]): Unit =
    currentFuture onComplete {
      _ =>
        this.currentFuture =
          f map {
            newState =>
              this.state = newState
          }
    }

  def pause(replyTo: ActorWire[PauseResponse, Unit]): Unit =
    executeSequential(ThrottleCompaction.pause(state, replyTo))

  override def paused(): Unit =
    executeSequential(ThrottleCompaction.pauseSuccessful(state))

  def resume(): Unit =
    executeSequential(ThrottleCompaction.resume(state))

  override def extensionSuccessful(): Unit =
    executeSequential(ThrottleCompaction.extensionSuccessful(state))

  override def extensionFailed(): Unit =
    executeSequential(ThrottleCompaction.extensionFailed(state))

  override def wakeUp(): Unit =
    executeSequential(ThrottleCompaction.wakeUp(state))
}

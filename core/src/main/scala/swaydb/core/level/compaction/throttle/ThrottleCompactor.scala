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

import com.typesafe.scalalogging.LazyLogging
import swaydb.DefActor
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.throttle.behaviour._
import swaydb.core.sweeper.FileSweeper
import swaydb.data.compaction.CompactionConfig.CompactionParallelism

import scala.concurrent.{ExecutionContext, Future}

/**
 * The compaction Actor state (subtype of [[Compactor]]) which gets
 * initialised under an [[DefActor]] via [[ThrottleCompactorCreator]].
 *
 * Implements all the compaction APIs and mutation is only managed here.
 */

object ThrottleCompactor {

  def apply(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor],
                                               behaviorWakeUp: BehaviorWakeUp,
                                               fileSweeper: FileSweeper.On,
                                               ec: ExecutionContext,
                                               parallelism: CompactionParallelism): ThrottleCompactor =
    new ThrottleCompactor(
      context = context,
      currentFuture = Future.unit,
      currentFutureExecuted = true
    )
}

private[core] class ThrottleCompactor private(@volatile private var context: ThrottleCompactorContext,
                                              @volatile private var currentFuture: Future[Unit],
                                              @volatile private var currentFutureExecuted: Boolean)(implicit self: DefActor[ThrottleCompactor],
                                                                                                    behaviour: BehaviorWakeUp,
                                                                                                    fileSweeper: FileSweeper.On,
                                                                                                    executionContext: ExecutionContext,
                                                                                                    parallelism: CompactionParallelism) extends Compactor with LazyLogging {
  @inline private def tailWakeUpCall(nextFuture: => Future[ThrottleCompactorContext]): Unit =
  //tail Future only if current future (wakeUp) was ignore else ignore. Not point tailing the same message.
    if (currentFutureExecuted) {
      currentFutureExecuted = false
      this.currentFuture =
        currentFuture
          .flatMap {
            _ =>
              //Set executed! allow the next wakeUp call to be accepted.
              currentFutureExecuted = true
              nextFuture map {
                newContext =>
                  this.context = newContext
              }
          }
          .recoverWith { //current future should never be a failed future
            case _ =>
              Future.unit
          }
    }

  override def wakeUp(): Unit =
    tailWakeUpCall(behaviour.wakeUp(context))

  def terminateASAP(): Unit =
    context.setTerminateASAP()
}

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

package swaydb.core.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.DefActor
import swaydb.core.compaction.Compactor
import swaydb.core.compaction.throttle.behaviour._
import swaydb.core.file.sweeper.FileSweeper
import swaydb.config.compaction.CompactionConfig.CompactionParallelism

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

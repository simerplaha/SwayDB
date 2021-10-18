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

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.throttle.behaviour.BehaviorWakeUp
import swaydb.core.level.compaction.{Compactor, CompactorCreator}
import swaydb.core.level.zero.LevelZero
import swaydb.core.sweeper.FileSweeper
import swaydb.data.compaction.CompactionConfig
import swaydb.data.slice.Slice
import swaydb.{Actor, DefActor, Error, IO}

import scala.concurrent.ExecutionContext

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */
private[core] object ThrottleCompactorCreator extends CompactorCreator with LazyLogging {

  /**
   * Creates compaction Actor
   */
  private def createCompactor(levels: Iterable[LevelRef],
                              config: CompactionConfig)(implicit fileSweeper: FileSweeper.On): DefActor[ThrottleCompactor] = {
    val state =
      ThrottleCompactorContext(
        levels = Slice(levels.toArray),
        compactionConfig = config,
        compactionStates = Map.empty
      )

    //ExecutionContext is shared by the Actor's executor and the Actor logic.
    implicit val ec: ExecutionContext = config.actorExecutionContext

    Actor.define[ThrottleCompactor](
      name = s"Compaction Actor",
      init = self =>
        ThrottleCompactor(state)(
          self = self,
          behaviorWakeUp = BehaviorWakeUp,
          fileSweeper = fileSweeper,
          ec = config.compactionExecutionContext,
          parallelism = config
        )
    ).onPreTerminate {
      case (impl, _) =>
        impl.terminateASAP()
    }.start()
  }

  private def createCompactor(zero: LevelZero,
                              config: CompactionConfig)(implicit fileSweeper: FileSweeper.On): IO[Error.Level, DefActor[ThrottleCompactor]] =
    zero.nextLevel match {
      case Some(nextLevel) =>
        logger.debug(s"Level(${zero.levelNumber}): Creating actor.")
        IO {
          createCompactor(
            levels = zero +: LevelRef.getLevels(nextLevel),
            config = config
          )
        }

      case None =>
        IO.Left(swaydb.Error.Fatal(new Exception("Compaction not started because there is no lower level.")))
    }

  def createAndListen(zero: LevelZero,
                      config: CompactionConfig)(implicit fileSweeper: FileSweeper.On): IO[Error.Level, DefActor[Compactor]] =
    createCompactor(
      zero = zero,
      config = config
    ) map {
      actor =>
        logger.debug(s"Level(${zero.levelNumber}): Initialising listener.")
        //listen to changes in levelZero
        zero onNextMapCallback (
          event =
            () =>
              actor.send(_.wakeUp())
          )

        actor
    }
}

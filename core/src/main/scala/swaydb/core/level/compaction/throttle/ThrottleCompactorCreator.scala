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
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.{Compactor, CompactorCreator}
import swaydb.core.level.zero.LevelZero
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
  def createCompactor(levels: Iterable[LevelRef],
                      config: CompactionConfig): DefActor[ThrottleCompactor, Unit] = {
    val state =
      ThrottleCompactorContext(
        levels = Slice(levels.toArray),
        resetCompactionPriorityAtInterval = config.resetCompactionPriorityAtInterval,
        compactionStates = Map.empty
      )

    implicit val ec: ExecutionContext = config.executionContext

    Actor.define[ThrottleCompactor](
      name = s"Compaction Actor",
      init = ThrottleCompactor(state)
    ).onPreTerminate {
      case (impl, _, _) =>
        impl.terminateASAP()
    }.start()
  }

  def createCompactor(zero: LevelZero,
                      config: CompactionConfig): IO[Error.Level, DefActor[ThrottleCompactor, Unit]] =
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
                      config: CompactionConfig): IO[Error.Level, DefActor[Compactor, Unit]] =
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

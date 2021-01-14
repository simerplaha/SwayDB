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
import swaydb.Error.Level.ExceptionHandler
import swaydb.IO._
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.{Compactor, CompactorCreator}
import swaydb.core.level.zero.LevelZero
import swaydb.data.NonEmptyList
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.slice.Slice
import swaydb.{Actor, ActorWire, Error, IO}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
 * Compactor = Compaction Actor.
 *
 * Implements Actor functions.
 */
private[core] object ThrottleCompactorCreator extends CompactorCreator with LazyLogging {

  /**
   * Split levels into compaction groups with dedicated or shared ExecutionContexts based on
   * the input [[CompactionExecutionContext]] config.
   *
   * @return return the root parent Actor with child Actors.
   */
  def createActors(levels: List[LevelRef],
                   executionContexts: List[CompactionExecutionContext])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                        locker: ActorWire[LastLevelLocker, Unit]): IO[swaydb.Error.Level, NonEmptyList[ActorWire[Compactor, Unit]]] =
    if (levels.size != executionContexts.size)
      IO.Left(swaydb.Error.Fatal(new IllegalStateException(s"Number of ExecutionContexts(${executionContexts.size}) is not the same as number of Levels(${levels.size}).")))
    else
      levels
        .zip(executionContexts)
        .foldLeftRecoverIO(ListBuffer.empty[(ListBuffer[LevelRef], ExecutionContext, Int)]) {
          case (jobs, (level, CompactionExecutionContext.Create(compactionEC, resetCompactionPriorityAtInterval))) => //new thread pool.
            jobs += ((ListBuffer(level), compactionEC, resetCompactionPriorityAtInterval))
            IO.Right(jobs)

          case (jobs, (level, CompactionExecutionContext.Shared)) => //share with previous thread pool.
            jobs.lastOption match {
              case Some((lastGroup, _, _)) =>
                lastGroup += level
                IO.Right(jobs)

              case None =>
                //this will never occur because during configuration Level0 is only allowed to have Create
                //so Shared can never happen with Create.
                IO.Left(swaydb.Error.Fatal(new IllegalStateException("Shared ExecutionContext submitted without Create.")))
            }
        }
        .flatMap {
          jobs =>
            val actors =
              jobs
                .zipWithIndex
                .foldRight(List.empty[ActorWire[Compactor, Unit]]) {
                  case (((jobs, executionContext, resetCompactionPriorityAtInterval), index), children) =>
                    val state =
                      ThrottleCompactorState(
                        levels = Slice(jobs.toArray),
                        resetCompactionPriorityAtInterval = resetCompactionPriorityAtInterval,
                        child = children.headOption,
                        compactionStates = Map.empty
                      )

                    val actor =
                      Actor.wire[ThrottleCompactor](
                        name = s"Compaction Actor$index",
                        init = ThrottleCompactor(state)
                      )(executionContext)

                    actor :: children
                }

            if (actors.isEmpty)
              IO.failed("Unable to create compactor(s).")
            else
              IO(NonEmptyList(actors.head, actors.tail))
        }

  def createCompactor(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                           locker: ActorWire[LastLevelLocker, Unit]): IO[Error.Level, NonEmptyList[ActorWire[Compactor, Unit]]] =
    zero.nextLevel match {
      case Some(nextLevel) =>
        logger.debug(s"Level(${zero.levelNumber}): Creating actor.")
        createActors(
          levels = zero +: LevelRef.getLevels(nextLevel),
          executionContexts = executionContexts
        )

      case None =>
        IO.Left(swaydb.Error.Fatal(new Exception("Compaction not started because there is no lower level.")))
    }

  def createAndListen(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                           locker: ActorWire[LastLevelLocker, Unit]): IO[Error.Level, NonEmptyList[ActorWire[Compactor, Unit]]] =
    createCompactor(
      zero = zero,
      executionContexts = executionContexts
    ) flatMap {
      actors =>
        logger.debug(s"Level(${zero.levelNumber}): Initialising listener.")
        //listen to changes in levelZero
        actors.headOption match {
          case Some(headActor) =>
            zero onNextMapCallback (
              event =
                () =>
                  headActor.send(_.wakeUp())
              )

            IO.Right(actors)

          case None =>
            IO.failed("No Actors created.")
        }
    }
}


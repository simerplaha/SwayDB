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
import swaydb.core.level._
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.level.compaction.task.assigner.{LevelTaskAssigner, LevelZeroTaskAssigner}
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleCompactorContext, ThrottleLevelOrdering, ThrottleLevelState}
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
private[throttle] object BehaviorWakeUp extends LazyLogging {

  def wakeUp(state: ThrottleCompactorContext)(implicit ec: ExecutionContext,
                                              self: DefActor[ThrottleCompactor, Unit]): Future[ThrottleCompactorContext] = {
    logger.debug(s"${state.name}: Wake-up successful!")
    runWakeUp(state)
      .recover {
        case exception =>
          logger.error("Failed compaction", exception)
          //continue with previous state
          state
      }
      .map(runPostCompaction)
  }

  private def runPostCompaction(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor, Unit]): ThrottleCompactorContext = {
    logger.debug(s"${context.name}: Wake-up successful!")
    scheduleWakeUp(context)
  }

  def fetchLastLevel(zero: LevelZero): Level = {
    var lastLevel: Level = null

    zero.nextLevel match {
      case Some(nextLevel) =>
        nextLevel.foreachLevel {
          case level: Level =>
            if (lastLevel == null || level.isNonEmpty())
              lastLevel = level

          case level =>
            throw new Exception(s"${level.getClass.getSimpleName} found in NextLevel.")
        }

      case None =>
        //Not sure if this should be supported in the future but currently at API
        //level we do not allow creating LevelZero without a NextLevel.
        throw new Exception(s"${LevelZero.productPrefix} with no lower level.")
    }

    if (lastLevel == null)
      throw new Exception("Last level is null.")

    lastLevel
  }

  private def runWakeUp(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor, Unit],
                                                           ec: ExecutionContext): Future[ThrottleCompactorContext] = {
    logger.debug(s"\n\n\n\n\n\n${context.name}: Running compaction!")

    Future
      .unit
      .mapUnit {
        context.levels.head match {
          case zero: LevelZero =>
            fetchLastLevel(zero)

          case _: NextLevel =>
            throw new Exception("Expected LevelZero")
        }
      }
      .flatMap {
        lockedLastLevel =>
          val levelsToCompact =
            context
              .levels
              .takeWhile(_.levelNumber != lockedLastLevel.levelNumber)
              .sorted(ThrottleLevelOrdering.ordering)

          //process only few job in the current thread and stop so that reordering occurs.
          //this is because processing all levels would take some time and during that time
          //level0 might fill up with level1 and level2 being empty and level0 maps not being
          //able to merged instantly.

          val compactions =
            if (context.compactionConfig.resetCompactionPriorityAtInterval < context.levels.size)
              levelsToCompact.take(context.compactionConfig.resetCompactionPriorityAtInterval)
            else
              levelsToCompact

          //run compaction jobs
          runCompactions(
            context = context,
            compactions = compactions,
            lockedLastLevel = lockedLastLevel
          )
      }
  }

  def shouldRun(level: LevelRef, newStateId: Long, state: ThrottleLevelState): Boolean =
    state match {
      case _: ThrottleLevelState.AwaitingExtension =>
        false

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        sleepDeadline.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())
    }

  def scheduleWakeUp(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor, Unit]): ThrottleCompactorContext = {
    logger.debug(s"${context.name}: scheduling next wakeup for updated state: ${context.levels.size}. Current scheduled: ${context.sleepTask.map(_._2.timeLeft.asString)}")

    val levelsToCompact =
      context
        .compactionStates
        .collect {
          case (level, levelState) if levelState.stateId != level.stateId || context.sleepTask.isEmpty =>
            (level, levelState)
        }

    logger.debug(s"${context.name}: Levels to compact: \t\n${levelsToCompact.map { case (level, state) => (level.levelNumber, state) }.mkString("\t\n")}")

    val nextDeadline =
      levelsToCompact.foldLeft(Option.empty[Deadline]) {
        case (nearestDeadline, (_, ThrottleLevelState.Sleeping(sleepDeadline, _))) =>
          FiniteDurations.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )

        case (nearestDeadline, (_, _: ThrottleLevelState.AwaitingExtension)) =>
          nearestDeadline
      }

    logger.debug(s"${context.name}: Time left for new deadline ${nextDeadline.map(_.timeLeft.asString)}")

    nextDeadline match {
      case Some(newWakeUpDeadline) =>
        //if the wakeUp deadlines are the same do not trigger another wakeUp.
        if (context.sleepTask.forall(_._2 > newWakeUpDeadline)) {
          context.sleepTask foreach (_._1.cancel())

          val newTask =
            self.send(newWakeUpDeadline.timeLeft) {
              (instance, _) =>
                instance.wakeUp()
            }

          logger.debug(s"${context.name}: Next wakeup scheduled!. Current scheduled: ${newWakeUpDeadline.timeLeft.asString}")
          context.copy(sleepTask = Some((newTask, newWakeUpDeadline)))
        } else {
          logger.debug(s"${context.name}: Some or later deadline. Ignoring re-scheduling. Keeping currently scheduled.")
          context
        }

      case None =>
        context
    }
  }

  private[throttle] def runCompactions(context: ThrottleCompactorContext,
                                       compactions: Slice[LevelRef],
                                       lockedLastLevel: Level)(implicit self: DefActor[ThrottleCompactor, Unit],
                                                               ec: ExecutionContext): Future[ThrottleCompactorContext] =
    if (context.terminateASAP()) {
      logger.warn(s"${context.name}: Cannot run jobs. Compaction is terminated.")
      Future.successful(context)
    } else {
      logger.debug(s"${context.name}: Compaction order: ${compactions.map(_.levelNumber).mkString(", ")}")

      val level = compactions.headOrNull

      if (level == null) {
        logger.debug(s"${context.name}: Compaction round complete.") //all jobs complete.
        Future.successful(context)
      } else {
        logger.debug(s"Level(${level.levelNumber}): ${context.name}: Running compaction.")
        val currentState = context.compactionStates.get(level)

        //Level's stateId should only be accessed here before compaction starts for the level.
        val stateId = level.stateId

        val nextLevels = compactions.dropHead().asInstanceOf[Slice[Level]]

        if ((currentState.isEmpty && level.nextCompactionDelay.fromNow.isOverdue()) || currentState.exists(state => shouldRun(level, stateId, state))) {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")

          runCompaction(
            level = level,
            nextLevels = nextLevels,
            stateId = stateId,
            lockedLastLevel = lockedLastLevel
          ) flatMap {
            nextState =>
              logger.debug(s"Level(${level.levelNumber}): ${context.name}: next state $nextState.")
              val updatedCompactionStates =
                context.compactionStates.updated(
                  key = level,
                  value = nextState
                )

              val newContext = context.copy(compactionStates = updatedCompactionStates)

              runCompactions(
                context = newContext,
                compactions = nextLevels,
                lockedLastLevel = lockedLastLevel
              )
          }

        } else {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: shouldRun = false.")
          runCompactions(
            context = context,
            compactions = nextLevels,
            lockedLastLevel = lockedLastLevel
          )
        }
      }
    }

  /**
   * It should be be re-fetched for the same compaction.
   */
  private[throttle] def runCompaction(level: LevelRef,
                                      nextLevels: Slice[Level],
                                      stateId: Long,
                                      lockedLastLevel: Level)(implicit ec: ExecutionContext,
                                                              self: DefActor[ThrottleCompactor, Unit]): Future[ThrottleLevelState] =
    level match {
      case zero: LevelZero =>
        compactLevelZero(
          zero = zero,
          nextLevels = nextLevels,
          stateId = stateId,
          lockedLastLevel = lockedLastLevel
        )

      case level: Level =>
        compactLevel(
          level = level,
          nextLevels = nextLevels,
          stateId = stateId,
          lockedLastLevel = lockedLastLevel
        )
    }

  private def compactLevelZero(zero: LevelZero,
                               nextLevels: Slice[Level],
                               stateId: Long,
                               lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[ThrottleLevelState] =
    if (zero.isEmpty)
      Future.successful {
        LevelSleepStates.success(
          zero = zero,
          stateId = stateId
        )
      }
    else
      LevelZeroTaskAssigner.run(
        source = zero,
        lowerLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead())
      ) flatMap {
        tasks =>
          BehaviourRunCompactionTask.runMapTask(
            task = tasks,
            lockedLastLevel = lockedLastLevel
          )
      } mapUnit {
        LevelSleepStates.success(
          zero = zero,
          stateId = stateId
        )
      } recover {
        case _ =>
          LevelSleepStates.failure(
            zero = zero,
            stateId = stateId
          )
      }

  private def compactLevel(level: Level,
                           nextLevels: Slice[Level],
                           stateId: Long,
                           lockedLastLevel: Level)(implicit ec: ExecutionContext,
                                                   self: DefActor[ThrottleCompactor, Unit]): Future[ThrottleLevelState] =
    if (level.isEmpty) {
      Future.successful {
        LevelSleepStates.success(
          level = level,
          stateId = stateId
        )
      }
    } else {
      def runTask(task: CompactionTask.Segments): Future[ThrottleLevelState] =
        BehaviourRunCompactionTask.runSegmentTask(
          task = task,
          lockedLastLevel = lockedLastLevel
        ) flatMapUnit {
          //if after running last Level compaction there is still an overflow request extension.
          if (level.levelNumber == lockedLastLevel.levelNumber && level.nextLevel.isDefined && level.nextCompactionDelay.fromNow.isOverdue()) {
            val task =
              LevelTaskAssigner.run(
                source = lockedLastLevel,
                lowerLevels = NonEmptyList(lockedLastLevel.nextLevel.get.asInstanceOf[Level]),
                sourceOverflow = lockedLastLevel.compactDataSize max lockedLastLevel.minSegmentSize
              )

            runTask(task)
          } else {
            Future.successful {
              LevelSleepStates.success(
                level = level,
                stateId = stateId
              )
            }
          }
        } recover {
          case _ =>
            LevelSleepStates.failure(
              level = level,
              stateId = stateId
            )
        }

      if (level.levelNumber == lockedLastLevel.levelNumber) {
        LevelTaskAssigner.cleanup(level = level, lockedLastLevel = lockedLastLevel) match {
          case Some(task) =>
            runTask(task)

          case None =>
            Future.successful {
              LevelSleepStates.success(
                level = level,
                stateId = stateId
              )
            }
        }
      } else {
        val task =
          LevelTaskAssigner.run(
            source = level,
            lowerLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead()),
            sourceOverflow = level.compactDataSize max level.minSegmentSize
          )

        runTask(task)
      }
    }
}
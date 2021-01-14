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
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleCompactorState, ThrottleLevelOrdering, ThrottleLevelState}
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
private[throttle] object ThrottleWakeUpBehavior extends LazyLogging {

  def wakeUp(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                            locker: ActorWire[LastLevelLocker, Unit],
                                            ec: ExecutionContext,
                                            self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] = {
    logger.debug(s"${state.name}: Wake-up successful!")
    runCompaction(state)
      .map(runPostCompaction)
      .recover {
        exception =>
          logger.error("Failed compaction", exception)
          runPostCompaction(state)
      }
  }

  private def runPostCompaction(context: ThrottleCompactorState)(implicit self: ActorWire[ThrottleCompactor, Unit]): ThrottleCompactorState = {
    logger.debug(s"${context.name}: Wake-up successful!")
    val updatedContext = scheduleWakeUp(context)
    wakeUpChild(updatedContext)
    updatedContext
  }

  private def runCompaction(context: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                             locker: ActorWire[LastLevelLocker, Unit],
                                                             self: ActorWire[ThrottleCompactor, Unit],
                                                             ec: ExecutionContext): Future[ThrottleCompactorState] = {
    logger.debug(s"\n\n\n\n\n\n${context.name}: Running compaction!")

    locker
      .ask
      .map {
        (impl, _) =>
          impl.lock()
      }
      .flatMap {
        lastLevel =>
          val levelsToCompact =
            context
              .levels
              .takeWhile(_.levelNumber != lastLevel.levelNumber)
              .sorted(ThrottleLevelOrdering.ordering)

          //process only few job in the current thread and stop so that reordering occurs.
          //this is because processing all levels would take some time and during that time
          //level0 might fill up with level1 and level2 being empty and level0 maps not being
          //able to merged instantly.

          val jobs =
            if (context.resetCompactionPriorityAtInterval < context.levels.size)
              levelsToCompact.take(context.resetCompactionPriorityAtInterval)
            else
              levelsToCompact

          //run compaction jobs
          runJobs(
            context = context,
            currentJobs = jobs,
            lastLevel = lastLevel
          )
      }
      .withCallback(locker.send(_.unlock()))
  }

  def shouldRun(level: LevelRef, newStateId: Long, state: ThrottleLevelState): Boolean =
    state match {
      case _: ThrottleLevelState.AwaitingExtension =>
        false

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        sleepDeadline.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())
    }

  def wakeUpChild(context: ThrottleCompactorState)(implicit self: ActorWire[ThrottleCompactor, Unit]): Unit = {
    logger.debug(s"${context.name}: Waking up child: ${context.child.map(_ => "child")}.")
    context.child.foreach(_.send(_.wakeUp()))
  }

  def scheduleWakeUp(context: ThrottleCompactorState)(implicit self: ActorWire[ThrottleCompactor, Unit]): ThrottleCompactorState = {
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

  private[throttle] def runJobs(context: ThrottleCompactorState,
                                currentJobs: Slice[LevelRef],
                                lastLevel: Level)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                  locker: ActorWire[LastLevelLocker, Unit],
                                                  self: ActorWire[ThrottleCompactor, Unit],
                                                  ec: ExecutionContext): Future[ThrottleCompactorState] =
    if (context.terminateASAP()) {
      logger.warn(s"${context.name}: Cannot run jobs. Compaction is terminated.")
      Future.successful(context)
    } else {
      logger.debug(s"${context.name}: Compaction order: ${currentJobs.map(_.levelNumber).mkString(", ")}")

      val level = currentJobs.headOrNull

      if (level == null) {
        logger.debug(s"${context.name}: Compaction round complete.") //all jobs complete.
        Future.successful(context)
      } else {
        logger.debug(s"Level(${level.levelNumber}): ${context.name}: Running compaction.")
        val currentState = context.compactionStates.get(level)

        //Level's stateId should only be accessed here before compaction starts for the level.
        val stateId = level.stateId

        val nextLevels = currentJobs.dropHead().asInstanceOf[Slice[Level]]

        if ((currentState.isEmpty && level.nextCompactionDelay.fromNow.isOverdue()) || currentState.exists(state => shouldRun(level, stateId, state))) {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")

          runJob(
            level = level,
            nextLevels = nextLevels,
            stateId = stateId,
            lastLevel = lastLevel
          ) flatMap {
            nextState =>
              logger.debug(s"Level(${level.levelNumber}): ${context.name}: next state $nextState.")
              val updatedCompactionStates =
                context.compactionStates.updated(
                  key = level,
                  value = nextState
                )

              val newContext = context.copy(compactionStates = updatedCompactionStates)

              runJobs(
                context = newContext,
                currentJobs = nextLevels,
                lastLevel = lastLevel
              )
          }

        } else {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: shouldRun = false.")
          runJobs(
            context = context,
            currentJobs = nextLevels,
            lastLevel = lastLevel
          )
        }
      }
    }

  /**
   * It should be be re-fetched for the same compaction.
   */
  private[throttle] def runJob(level: LevelRef,
                               nextLevels: Slice[Level],
                               stateId: Long,
                               lastLevel: Level)(implicit ec: ExecutionContext,
                                                 locker: ActorWire[LastLevelLocker, Unit],
                                                 self: ActorWire[ThrottleCompactor, Unit],
                                                 committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    level match {
      case zero: LevelZero =>
        compactLevelZero(
          zero = zero,
          nextLevels = nextLevels,
          stateId = stateId,
          lastLevel = lastLevel
        )

      case level: Level =>
        compactLevel(
          level = level,
          nextLevels = nextLevels,
          stateId = stateId,
          lastLevel = lastLevel
        )
    }

  private def compactLevelZero(zero: LevelZero,
                               nextLevels: Slice[Level],
                               stateId: Long,
                               lastLevel: Level)(implicit ec: ExecutionContext,
                                                 committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    if (zero.isEmpty)
      Future.successful {
        LevelSleepState.success(
          zero = zero,
          stateId = stateId
        )
      }
    else
      CompactionLevelZeroTasker.run(
        source = zero,
        lowerLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead())
      ) flatMap {
        tasks =>
          runMapTask(
            task = tasks,
            lastLevel = lastLevel
          )
      } mapUnit {
        LevelSleepState.success(
          zero = zero,
          stateId = stateId
        )
      } recover {
        _ =>
          LevelSleepState.failure(
            zero = zero,
            stateId = stateId
          )
      }

  private def compactLevel(level: Level,
                           nextLevels: Slice[Level],
                           stateId: Long,
                           lastLevel: Level)(implicit ec: ExecutionContext,
                                             locker: ActorWire[LastLevelLocker, Unit],
                                             self: ActorWire[ThrottleCompactor, Unit],
                                             committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    if (level.isEmpty) {
      Future.successful {
        LevelSleepState.success(
          level = level,
          stateId = stateId
        )
      }
    } else {
      val tasks =
        if (nextLevels.isEmpty)
          CompactionLevelTasker.run(source = level)
        else
          CompactionLevelTasker.run(
            source = level,
            nextLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead()),
            sourceOverflow = level.compactDataSize
          )

      runSegmentTask(
        task = tasks,
        lastLevel = lastLevel
      ) mapUnit {
        //if after running last Level compaction there is still an overflow request extension.
        if (nextLevels.isEmpty && level.nextLevel.isDefined && level.nextCompactionDelay.fromNow.isOverdue()) {
          locker.send(_.set(level.nextLevel.get.asInstanceOf[Level], self))
          ThrottleLevelState.AwaitingExtension(stateId)
        } else {
          LevelSleepState.success(
            level = level,
            stateId = stateId
          )
        }
      } recover {
        _ =>
          LevelSleepState.failure(
            level = level,
            stateId = stateId
          )
      }
    }

  private[throttle] def runSegmentTask(task: CompactionTask.Segments,
                                       lastLevel: Level)(implicit ec: ExecutionContext,
                                                         committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        runSegmentTask(task = task, lastLevel = lastLevel)

      case task: CompactionTask.CollapseSegments =>
        runSegmentTask(task = task, lastLevel = lastLevel)

      case task: CompactionTask.RefreshSegments =>
        runSegmentTask(task = task, lastLevel = lastLevel)
    }

  private[throttle] def runSegmentTask(task: CompactionTask.CompactSegments,
                                       lastLevel: Level)(implicit ec: ExecutionContext,
                                                         committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          task.targetLevel.merge(task.data, removeDeletedRecords = task.targetLevel.levelNumber == lastLevel.levelNumber) map {
            result =>
              (task.targetLevel, result)
          }
      } flatMap {
        result =>
          committer
            .ask
            .flatMap {
              (impl, _) =>
                impl.commit(
                  fromLevel = task.targetLevel,
                  segments = task.tasks.flatMap(_.data),
                  mergeResults = result
                ).toFuture
            }
      }

  private[throttle] def runSegmentTask(task: CompactionTask.CollapseSegments,
                                       lastLevel: Level)(implicit ec: ExecutionContext,
                                                         committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .targetLevel
        .collapse(segments = task.segments, removeDeletedRecords = task.targetLevel.levelNumber == lastLevel.levelNumber)
        .flatMap {
          case LevelCollapseResult.Empty =>
            Future.failed(new Exception(s"Collapse failed: ${LevelCollapseResult.productPrefix}.${LevelCollapseResult.Empty.productPrefix}"))

          case LevelCollapseResult.Collapsed(sourceSegments, mergeResult) =>
            committer
              .ask
              .flatMap {
                (impl, _) =>
                  impl.replace(
                    level = task.targetLevel,
                    old = sourceSegments,
                    result = mergeResult
                  ).toFuture
              }
        }

  private[throttle] def runSegmentTask(task: CompactionTask.RefreshSegments,
                                       lastLevel: Level)(implicit ec: ExecutionContext,
                                                         committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .targetLevel
        .refresh(segments = task.segments, removeDeletedRecords = task.targetLevel.levelNumber == lastLevel.levelNumber)
        .toFuture //execute on current thread.
        .flatMap {
          result =>
            committer
              .ask
              .flatMap {
                (impl, _) =>
                  impl.commit(
                    level = task.targetLevel,
                    result = result
                  ).toFuture
              }
        }

  private[throttle] def runMapTask(task: CompactionTask.CompactMaps,
                                   lastLevel: Level)(implicit ec: ExecutionContext,
                                                     committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.maps.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          task.targetLevel.merge(
            segments = task.data,
            removeDeletedRecords = task.targetLevel.levelNumber == lastLevel.levelNumber
          ) map {
            result =>
              (task.targetLevel, result)
          }
      } flatMap {
        result =>
          committer
            .ask
            .flatMap {
              (impl, _) =>
                impl.commit(
                  fromLevel = task.targetLevel,
                  maps = task.maps,
                  mergeResults = result
                ).toFuture
            }
      }
}

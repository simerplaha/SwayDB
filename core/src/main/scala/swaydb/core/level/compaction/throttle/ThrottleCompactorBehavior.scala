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
import swaydb.core.level._
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.lock.LastLevelLocker
import swaydb.core.level.compaction.task.{CompactionLevelTasker, CompactionLevelZeroTasker, CompactionTask}
import swaydb.core.level.compaction.throttle.ThrottleCompactor.PauseResponse
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
private[throttle] object ThrottleCompactorBehavior extends LazyLogging {

  val awaitPullTimeout = 6.seconds

  def requestWakeUp(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                   locker: ActorWire[LastLevelLocker, Unit],
                                                   ec: ExecutionContext,
                                                   self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    state match {
      case _: ThrottleCompactorState.Terminated =>
        logger.error(s"${state.name}: Wake-up failed because compaction is terminated!")
        Future.successful(state)

      case state: ThrottleCompactorState.Paused =>
        logger.error(s"${state.name}: Wake-up failed because compaction is Paused!")
        Future.successful(state)

      case state: ThrottleCompactorState.Sleeping =>
        logger.debug(s"${state.name}: Wake-up successful!")
        runCompaction(state.context)
    }

  def requestPause(state: ThrottleCompactorState,
                   replyTo: ActorWire[PauseResponse, Unit])(implicit self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    state match {
      case _: ThrottleCompactorState.Terminated =>
        logger.error(s"${state.name}: Pause failed because compaction is terminated!")
        replyTo.send(_.pauseFailed(self))
        Future.successful(state)

      case _: ThrottleCompactorState.Paused =>
        logger.debug(s"${state.name}: Already paused!")
        replyTo.send(_.pauseSuccessful(self))
        Future.successful(state)

      case state: ThrottleCompactorState.Sleeping =>
        replyTo.send(_.pauseSuccessful(self))
        Future.successful(ThrottleCompactorState.Paused(state))
    }

  def clearPaused(from: ActorWire[PauseResponse, Unit],
                  state: ThrottleCompactorState.Idle)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                      locker: ActorWire[LastLevelLocker, Unit],
                                                      ec: ExecutionContext,
                                                      self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] = {
    logger.debug(s"${state.name}: Pause successful.")

    val compactionStates = state.context.compactionStates.toList

    val removed =
      compactionStates.foldLeft(false) {
        case (removed, (level, awaiting: ThrottleLevelState.AwaitingPause)) =>
          //execute remove first so that all awaits a removed.
          val removeResult = awaiting.fromActors.remove(from)

          if (awaiting.fromActors.isEmpty)
            state.context.compactionStates.put(level, ThrottleLevelState.Sleeping(level.nextCompactionDelay.fromNow, awaiting.stateId))

          removeResult || removed
      }

    if (!removed) {
      logger.error("Unexpected pause successful request")
      Future.successful(state)
    } else {
      runCompaction(state.context)
    }
  }

  def pauseSuccessful(from: ActorWire[PauseResponse, Unit],
                      state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                     locker: ActorWire[LastLevelLocker, Unit],
                                                     ec: ExecutionContext,
                                                     self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    state match {
      case _: ThrottleCompactorState.Terminated =>
        logger.debug(s"${state.name}: Terminated!")
        Future.successful(state)

      case state: ThrottleCompactorState.Idle =>
        logger.debug(s"${state.name}: Pause successful.")

        val compactionStates = state.context.compactionStates.toList

        val removed =
          compactionStates.foldLeft(false) {
            case (removed, (level, awaiting: ThrottleLevelState.AwaitingPause)) =>
              //execute remove first so that all awaits a removed.
              val removeResult = awaiting.fromActors.remove(from)

              if (awaiting.fromActors.isEmpty)
                state.context.compactionStates.put(level, ThrottleLevelState.Sleeping(level.nextCompactionDelay.fromNow, awaiting.stateId))

              removeResult || removed
          }

        if (!removed) {
          logger.error("Unexpected pause successful request")
          Future.successful(state)
        } else {
          runCompaction(state.context)
        }
    }

  def pauseFailed(from: ActorWire[PauseResponse, Unit],
                  state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                 locker: ActorWire[LastLevelLocker, Unit],
                                                 ec: ExecutionContext,
                                                 self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    state match {
      case _: ThrottleCompactorState.Terminated =>
        logger.debug(s"${state.name}: Terminated!")
        Future.successful(state)

      case state: ThrottleCompactorState.Idle =>
        logger.debug(s"${state.name}: Pause successful.")

        val removed =
          state.context.compactionStates.foldLeft(false) {
            case (removed, (_, state: ThrottleLevelState.AwaitingPause)) =>
              //execute remove first so that all awaits a removed.
              state.fromActors.remove(from) || removed
          }

        if (!removed)
          logger.error("Unexpected pause successful request")

        Future.successful(state)
    }

  def requestResume(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                   locker: ActorWire[LastLevelLocker, Unit],
                                                   ec: ExecutionContext,
                                                   self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    ???

  def extensionSuccessful(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                         locker: ActorWire[LastLevelLocker, Unit],
                                                         ec: ExecutionContext,
                                                         self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    ???

  def extensionFailed(state: ThrottleCompactorState)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                     locker: ActorWire[LastLevelLocker, Unit],
                                                     ec: ExecutionContext,
                                                     self: ActorWire[ThrottleCompactor, Unit]): Future[ThrottleCompactorState] =
    ???


  private[throttle] def runCompaction(state: ThrottleCompactorState.Context)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                             ec: ExecutionContext): Future[ThrottleCompactorState] = {
    logger.debug(s"\n\n\n\n\n\n${state.name}: Running compaction now!")

    val levels = state.levels.sorted(state.ordering)

    //process only few job in the current thread and stop so that reordering occurs.
    //this is because processing all levels would take some time and during that time
    //level0 might fill up with level1 and level2 being empty and level0 maps not being
    //able to merged instantly.

    val jobs =
      if (state.resetCompactionPriorityAtInterval < state.levels.size)
        levels.take(state.resetCompactionPriorityAtInterval)
      else
        levels

    //run compaction jobs
    runJobs(
      context = state,
      currentJobs = jobs
    )
  }

  def shouldRun(level: LevelRef, newStateId: Long, state: ThrottleLevelState): Boolean =
    state match {
      case awaitingState: ThrottleLevelState.AwaitingState =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        newStateId != awaitingState.stateId && level.nextCompactionDelay.fromNow.isOverdue()

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        sleepDeadline.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())
    }

  def wakeUpChild(state: ThrottleCompactorState.Context)(implicit self: ActorWire[ThrottleCompactor, Unit]): Unit = {
    logger.debug(s"${state.name}: Waking up child: ${state.child.map(_ => "child")}.")
    state.child.foreach(_.send(_.wakeUp()))
  }

  def scheduleWakeUp(state: ThrottleCompactorState.Context)(implicit self: ActorWire[ThrottleCompactor, Unit]): Unit = {
    logger.debug(s"${state.name}: scheduling next wakeup for updated state: ${state.levels.size}. Current scheduled: ${state.sleepTask.map(_._2.timeLeft.asString)}")

    val levelsToCompact =
      state
        .compactionStates
        .collect {
          case (level, levelState) if levelState.stateId != level.stateId || state.sleepTask.isEmpty =>
            (level, levelState)
        }

    logger.debug(s"${state.name}: Levels to compact: \t\n${levelsToCompact.map { case (level, state) => (level.levelNumber, state) }.mkString("\t\n")}")

    val nextDeadline =
      levelsToCompact.foldLeft(Option.empty[Deadline]) {
        case (nearestDeadline, (_, ThrottleLevelState.Sleeping(sleepDeadline, _))) =>
          FiniteDurations.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )

        case (nearestDeadline, (_, _: ThrottleLevelState.AwaitingState)) =>
          nearestDeadline
      }

    logger.debug(s"${state.name}: Time left for new deadline ${nextDeadline.map(_.timeLeft.asString)}")

    nextDeadline
      .foreach {
        newWakeUpDeadline =>
          //if the wakeUp deadlines are the same do not trigger another wakeUp.
          if (state.sleepTask.forall(_._2 > newWakeUpDeadline)) {
            state.sleepTask foreach (_._1.cancel())

            val newTask =
              self.send(newWakeUpDeadline.timeLeft) {
                (instance, _) =>
                  state.sleepTask = None
                  instance.wakeUp()
              }

            state.sleepTask = Some((newTask, newWakeUpDeadline))
            logger.debug(s"${state.name}: Next wakeup scheduled!. Current scheduled: ${newWakeUpDeadline.timeLeft.asString}")
          } else {
            logger.debug(s"${state.name}: Some or later deadline. Ignoring re-scheduling. Keeping currently scheduled.")
          }
      }
  }

  private[throttle] def runJobs(context: ThrottleCompactorState.Context,
                                currentJobs: Slice[LevelRef])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                              ec: ExecutionContext): Future[ThrottleCompactorState] =
    if (context.stopped) {
      logger.warn(s"${context.name}: Cannot run jobs. Compaction is terminated.")
      Future.successful(ThrottleCompactorState.Sleeping(context))
    } else {
      logger.debug(s"${context.name}: Compaction order: ${currentJobs.map(_.levelNumber).mkString(", ")}")

      val level = currentJobs.headOrNull

      if (level == null) {
        logger.debug(s"${context.name}: Compaction round complete.") //all jobs complete.
        Future.successful(ThrottleCompactorState.Sleeping(context))
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
            stateId = stateId
          ) flatMap {
            nextState =>
              logger.debug(s"Level(${level.levelNumber}): ${context.name}: next state $nextState.")
              context.compactionStates.put(
                key = level,
                value = nextState
              )

              runJobs(context, nextLevels)
          }

        } else {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: shouldRun = false.")
          runJobs(context, nextLevels)
        }
      }
    }

  /**
   * It should be be re-fetched for the same compaction.
   */
  private[throttle] def runJob(level: LevelRef,
                               nextLevels: Slice[Level],
                               stateId: Long)(implicit ec: ExecutionContext,
                                              committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    level match {
      case zero: LevelZero =>
        pushForward(
          zero = zero,
          nextLevels = nextLevels,
          stateId = stateId
        )

      case level: Level =>
        pushForward(
          level = level,
          nextLevels = nextLevels,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    nextLevels: Slice[Level],
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    runTasks(
      zero = zero,
      nextLevels = nextLevels,
      stateId = stateId
    ) mapUnit {
      logger.debug(s"LevelZero: pushed Maps.")
      ThrottleLevelState.Sleeping(
        sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
        stateId = stateId
      )
    } recover {
      case _ =>
        ThrottleLevelState.Sleeping(
          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else (ThrottleLevelState.failureSleepDuration min zero.nextCompactionDelay).fromNow,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(level: Level,
                                    nextLevels: Slice[Level],
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    runTasks(
      level = level,
      nextLevels = nextLevels,
      stateId = stateId
    ) mapUnit {
      logger.debug(s"Level(${level.levelNumber}): pushed Segments.")
      ThrottleLevelState.Sleeping(
        sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else level.nextCompactionDelay.fromNow,
        stateId = stateId
      )
    } recover {
      case _ =>
        ThrottleLevelState.Sleeping(
          sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else (ThrottleLevelState.failureSleepDuration min level.nextCompactionDelay).fromNow,
          stateId = stateId
        )
    }

  private def runTasks(zero: LevelZero,
                       nextLevels: Slice[Level],
                       stateId: Long)(implicit ec: ExecutionContext,
                                      committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (zero.isEmpty)
      Future.unit
    else
      CompactionLevelZeroTasker.run(
        source = zero,
        lowerLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead())
      ) flatMap {
        tasks =>
          runMapTask(tasks)
      }

  private def runTasks(level: Level,
                       nextLevels: Slice[Level],
                       stateId: Long)(implicit ec: ExecutionContext,
                                      committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (level.isEmpty) {
      Future.unit
    } else {
      val tasks =
        if (nextLevels.isEmpty)
          CompactionLevelTasker.run(
            source = level,
            sourceOverflow = 10000
          )
        else
          CompactionLevelTasker.run(
            source = level,
            nextLevels = NonEmptyList(nextLevels.head, nextLevels.dropHead()),
            sourceOverflow = 10000
          )

      runSegmentTask(tasks)
    }

  private[throttle] def runSegmentTask(task: CompactionTask.Segments)(implicit ec: ExecutionContext,
                                                                      committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        runSegmentTask(task)

      case task: CompactionTask.CollapseSegments =>
        runSegmentTask(task)

      case task: CompactionTask.RefreshSegments =>
        runSegmentTask(task)
    }

  private[throttle] def runSegmentTask(task: CompactionTask.CompactSegments)(implicit ec: ExecutionContext,
                                                                             committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          task.targetLevel.merge(task.data, removeDeletedRecords = false) map {
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
                  fromLevel = task.sourceLevel,
                  segments = task.tasks.flatMap(_.data),
                  mergeResults = result
                ).toFuture
            }
      }

  private[throttle] def runSegmentTask(task: CompactionTask.CollapseSegments)(implicit ec: ExecutionContext,
                                                                              committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .level
        .collapse(segments = task.segments, removeDeletedRecords = false)
        .flatMap {
          case LevelCollapseResult.Empty =>
            Future.failed(new Exception(s"Collapse failed: ${LevelCollapseResult.productPrefix}.${LevelCollapseResult.Empty.productPrefix}"))

          case LevelCollapseResult.Collapsed(sourceSegments, mergeResult) =>
            committer
              .ask
              .flatMap {
                (impl, _) =>
                  impl.replace(
                    level = task.level,
                    old = sourceSegments,
                    result = mergeResult
                  ).toFuture
              }
        }

  private[throttle] def runSegmentTask(task: CompactionTask.RefreshSegments)(implicit ec: ExecutionContext,
                                                                             committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .level
        .refresh(segments = task.segments, removeDeletedRecords = false) //execute on current thread.
        .toFuture
        .flatMap {
          result =>
            committer
              .ask
              .flatMap {
                (impl, _) =>
                  impl.commit(
                    level = task.level,
                    result = result
                  ).toFuture
              }
        }

  private[throttle] def runMapTask(task: CompactionTask.CompactMaps)(implicit ec: ExecutionContext,
                                                                     committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.maps.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          task.targetLevel.merge(task.data, removeDeletedRecords = false) map {
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
                  fromLevel = task.levelZero,
                  maps = task.maps,
                  mergeResults = result
                ).toFuture
            }
      }
}

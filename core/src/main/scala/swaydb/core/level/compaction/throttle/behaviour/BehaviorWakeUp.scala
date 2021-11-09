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

package swaydb.core.level.compaction.throttle.behaviour

import com.typesafe.scalalogging.LazyLogging
import swaydb.DefActor
import swaydb.core.level._
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.level.compaction.task.assigner.{LevelTaskAssigner, LevelZeroTaskAssigner}
import swaydb.core.level.compaction.throttle.{LevelState, ThrottleCompactor, ThrottleCompactorContext, ThrottleLevelOrdering}
import swaydb.core.level.zero.LevelZero
import swaydb.core.sweeper.FileSweeper
import swaydb.data.NonEmptyList
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.compaction.PushStrategy
import swaydb.data.slice.Slice
import swaydb.utils.FiniteDurations
import swaydb.utils.FiniteDurations.FiniteDurationImplicits
import swaydb.utils.Futures.{FutureUnitImplicits, _}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[throttle] trait BehaviorWakeUp {

  def wakeUp(context: ThrottleCompactorContext)(implicit ec: ExecutionContext,
                                                self: DefActor[ThrottleCompactor],
                                                fileSweeper: FileSweeper.On,
                                                parallelism: CompactionParallelism): Future[ThrottleCompactorContext]

}

/**
 * Implements compaction functions.
 */
private[throttle] object BehaviorWakeUp extends BehaviorWakeUp with LazyLogging {

  def wakeUp(context: ThrottleCompactorContext)(implicit ec: ExecutionContext,
                                                self: DefActor[ThrottleCompactor],
                                                fileSweeper: FileSweeper.On,
                                                parallelism: CompactionParallelism): Future[ThrottleCompactorContext] = {
    logger.debug(s"\n\n\n\n\n\n${context.name}: Wakeup - Running compaction!")

    runWakeUp(context)
      .recover {
        case exception =>
          logger.error("Failed compaction", exception)
          //continue with previous state
          context
      }
      .map(runPostCompaction)
  }

  private def runPostCompaction(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor]): ThrottleCompactorContext = {
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

  private def runWakeUp(context: ThrottleCompactorContext)(implicit ec: ExecutionContext,
                                                           fileSweeper: FileSweeper.On,
                                                           parallelism: CompactionParallelism): Future[ThrottleCompactorContext] =
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
        lastLevel =>
          val levelsToCompactInOrder =
            context
              .levels
              .takeWhile(_.levelNumber <= lastLevel.levelNumber)
              .sorted(ThrottleLevelOrdering.ordering)

          //process only few job in the current thread and stop so that reordering occurs.
          //this is because processing all levels would take some time and during that time
          //level0 might fill up with level1 and level2 being empty and level0 maps not being
          //able to merged instantly.

          val compactionsOrder =
            if (context.compactionConfig.resetCompactionPriorityAtInterval < context.levels.size)
              levelsToCompactInOrder.take(context.compactionConfig.resetCompactionPriorityAtInterval)
            else
              levelsToCompactInOrder

          //run compaction jobs
          runCompactions(
            context = context,
            compactionsOrder = compactionsOrder,
            lastLevel = lastLevel
          )
      }

  def shouldRun(level: LevelRef, newStateId: Long, state: LevelState.Sleeping): Boolean = {
    logger.debug(s"Level(${level.levelNumber}): $state")
    state.sleepDeadline.isOverdue() || (newStateId != state.stateId && level.nextCompactionDelay.fromNow.isOverdue())
  }

  def scheduleWakeUp(context: ThrottleCompactorContext)(implicit self: DefActor[ThrottleCompactor]): ThrottleCompactorContext = {
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
        case (nearestDeadline, (_, LevelState.Sleeping(sleepDeadline, _))) =>
          FiniteDurations.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )
      }

    logger.debug(s"${context.name}: Time left for new deadline ${nextDeadline.map(_.timeLeft.asString)}")

    nextDeadline match {
      case Some(newWakeUpDeadline) =>
        //if the wakeUp deadlines are the same do not trigger another wakeUp.
        if (context.sleepTask.forall(_._2 > newWakeUpDeadline)) {
          context.sleepTask foreach (_._1.cancel())

          val newTask = self.send(newWakeUpDeadline.timeLeft)(_.wakeUp())

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

  def runCompactions(context: ThrottleCompactorContext,
                     compactionsOrder: Slice[LevelRef],
                     lastLevel: Level)(implicit ec: ExecutionContext,
                                       fileSweeper: FileSweeper.On,
                                       parallelism: CompactionParallelism): Future[ThrottleCompactorContext] =
    if (context.terminateASAP()) {
      logger.warn(s"${context.name}: Cannot run jobs. Compaction is terminated.")
      Future.successful(context)
    } else {
      logger.debug(s"${context.name}: Compaction order: ${compactionsOrder.mapToSlice(_.levelNumber).mkString(", ")}")

      val level = compactionsOrder.headOrNull

      if (level == null) {
        logger.debug(s"${context.name}: Compaction round complete.") //all jobs complete.
        Future.successful(context)
      } else {
        logger.debug(s"Level(${level.levelNumber}): ${context.name}: Running compaction.")
        val currentState = context.compactionStates.get(level)

        //Level's stateId should only be accessed here before compaction starts for the level.
        val stateId = level.stateId

        if ((currentState.isEmpty && level.nextCompactionDelay.fromNow.isOverdue()) || currentState.exists(state => shouldRun(level, stateId, state))) {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")

          runCompaction(
            level = level,
            stateId = stateId,
            lastLevel = lastLevel,
            pushStrategy = context.compactionConfig.pushStrategy
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
                compactionsOrder = compactionsOrder.dropHead(),
                lastLevel = lastLevel
              )
          }

        } else {
          logger.debug(s"Level(${level.levelNumber}): ${context.name}: shouldRun = false.")
          runCompactions(
            context = context,
            compactionsOrder = compactionsOrder.dropHead(),
            lastLevel = lastLevel
          )
        }
      }
    }

  /**
   * It should be be re-fetched for the same compaction.
   */
  def runCompaction(level: LevelRef,
                    stateId: Long,
                    lastLevel: Level,
                    pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                fileSweeper: FileSweeper.On,
                                                parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    level match {
      case zero: LevelZero =>
        compactLevelZero(
          zero = zero,
          stateId = stateId,
          lastLevel = lastLevel,
          pushStrategy = pushStrategy
        )

      case level: Level =>
        compactLevel(
          level = level,
          stateId = stateId,
          lastLevel = lastLevel,
          pushStrategy = pushStrategy
        )
    }

  def buildLowerLevels(level: LevelRef, lastLevel: Level): NonEmptyList[Level] = {
    val nextLevel = level.nextLevel.get.asInstanceOf[Level]

    val lowerLevels =
      nextLevel.nextLevels.collect {
        case level: Level if level.levelNumber <= lastLevel.levelNumber => level
      }

    NonEmptyList(nextLevel, lowerLevels)
  }

  def compactLevelZero(zero: LevelZero,
                       stateId: Long,
                       lastLevel: Level,
                       pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                   fileSweeper: FileSweeper.On,
                                                   parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    if (zero.isEmpty)
      LevelSleepStates.success(
        zero = zero,
        stateId = stateId
      ).toFuture
    else
      LevelZeroTaskAssigner.run(
        source = zero,
        pushStrategy = pushStrategy,
        lowerLevels = buildLowerLevels(zero, lastLevel)
      ) flatMap {
        tasks =>
          BehaviourCompactionTask.compactLogs(
            task = tasks,
            lastLevel = lastLevel
          )
      } mapUnit {
        LevelSleepStates.success(
          zero = zero,
          stateId = stateId
        )
      } onError {
        LevelSleepStates.failure(
          stateId = stateId
        )
      }

  def compactLevel(level: Level,
                   stateId: Long,
                   lastLevel: Level,
                   pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                               fileSweeper: FileSweeper.On,
                                               parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    if (level.isEmpty)
      LevelSleepStates.success(
        level = level,
        stateId = stateId
      ).toFuture
    else if (level.levelNumber == lastLevel.levelNumber)
      compactLastLevel(
        level = level,
        stateId = stateId,
        pushStrategy = pushStrategy
      )
    else
      compactUpperLevel(
        level = level,
        stateId = stateId,
        lastLevel = lastLevel,
        pushStrategy = pushStrategy
      )

  def compactUpperLevel(level: Level,
                        stateId: Long,
                        lastLevel: Level,
                        pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                    fileSweeper: FileSweeper.On,
                                                    parallelism: CompactionParallelism): Future[LevelState.Sleeping] = {
    val task =
      LevelTaskAssigner.assign(
        source = level,
        pushStrategy = pushStrategy,
        lowerLevels = buildLowerLevels(level, lastLevel),
        sourceOverflow = level.compactDataSize max level.minSegmentSize
      )

    val taskResult =
      runSegmentTask(
        task = task,
        level = level,
        stateId = stateId,
        lastLevel = lastLevel,
        pushStrategy = pushStrategy
      )

    taskResult and {
      LevelTaskAssigner.collapse(level = level) match {
        case Some(task) =>
          runSegmentTask(
            task = task,
            level = level,
            stateId = stateId,
            lastLevel = lastLevel,
            pushStrategy = pushStrategy
          )

        case None =>
          taskResult
      }
    }
  }

  def compactLastLevel(level: Level,
                       stateId: Long,
                       pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                   fileSweeper: FileSweeper.On,
                                                   parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    LevelTaskAssigner.refresh(level = level) match {
      case Some(task) =>
        //ignore extendLastLevelMayBe if only refresh was executed
        //and wait for next compaction cycle to check this again
        runSegmentTask(
          task = task,
          level = level,
          stateId = stateId,
          lastLevel = level,
          pushStrategy = pushStrategy
        )

      case None =>
        //run extendLastLevelMayBe only after collapse
        LevelTaskAssigner.collapse(level = level) match {
          case Some(task) =>
            runSegmentTask(
              task = task,
              level = level,
              stateId = stateId,
              lastLevel = level,
              pushStrategy = pushStrategy
            ) flatMap {
              state =>
                extendLastLevelMayBe(
                  newState = state,
                  stateId = stateId,
                  level = level,
                  pushStrategy = pushStrategy
                )
            }

          case None =>
            val state =
              LevelSleepStates.success(
                level = level,
                stateId = stateId
              )

            extendLastLevelMayBe(
              newState = state,
              stateId = stateId,
              level = level,
              pushStrategy = pushStrategy
            )
        }
    }

  /**
   * Pre-condition:
   * LastLevel runs refresh and collapse as part of it's compaction process.
   * If refresh is executed collapse is executed in the next compaction cycle
   * to re-prioritise Level. So this function should only be invoked after a
   * full cleanup cycle i.e. after [[LevelTaskAssigner.collapse]] is invoked.
   *
   * Execution condition:
   * If collapse was executed and there are no more cleanups remaining
   * but the level is still overflowing (indicated by newState's sleepDeadline)
   * only then extend lowerLevel to next level by pushing segment.
   */
  def extendLastLevelMayBe(newState: LevelState.Sleeping,
                           stateId: Long,
                           level: Level,
                           pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                       fileSweeper: FileSweeper.On,
                                                       parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    if (level.nextLevel.isDefined && newState.sleepDeadline.isOverdue() && LevelTaskAssigner.cleanup(level).isEmpty) {
      val task =
        LevelTaskAssigner.assign(
          source = level,
          pushStrategy = pushStrategy,
          lowerLevels = NonEmptyList(level.nextLevel.get.asInstanceOf[Level]),
          sourceOverflow = level.compactDataSize max level.minSegmentSize
        )

      runSegmentTask(
        task = task,
        level = level,
        stateId = stateId,
        lastLevel = level,
        pushStrategy = pushStrategy
      )
    } else {
      Future.successful(newState)
    }

  def runSegmentTask(task: CompactionTask.Segments,
                     level: Level,
                     stateId: Long,
                     lastLevel: Level,
                     pushStrategy: PushStrategy)(implicit ec: ExecutionContext,
                                                 fileSweeper: FileSweeper.On,
                                                 parallelism: CompactionParallelism): Future[LevelState.Sleeping] =
    BehaviourCompactionTask.runSegmentTask(
      task = task,
      lastLevel = lastLevel
    ) mapUnit {
      LevelSleepStates.success(
        level = level,
        stateId = stateId
      )
    } onError {
      LevelSleepStates.failure(
        stateId = stateId
      )
    }
}

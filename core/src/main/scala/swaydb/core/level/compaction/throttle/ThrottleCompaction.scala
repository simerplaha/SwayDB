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
import swaydb.core.data.Memory
import swaydb.core.level._
import swaydb.core.level.compaction.Compaction
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.selector.{CompactionSegmentSelector, CompactionTask}
import swaydb.core.level.zero.{LevelZero, LevelZeroMapCache}
import swaydb.data.slice.Slice
import swaydb.data.util.Futures._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
 * [[ThrottleCompaction]] does not implement any concurrency which should be handled by an Actor (see [[ThrottleCompactor.createActors]]).
 *
 * This just implements compaction functions that mutate the state ([[ThrottleState]]) of Levels by executing compaction functions
 * of a Level.
 *
 * This process cannot be immutable because we want to minimise garbage workload during compaction.
 */
private[throttle] object ThrottleCompaction extends Compaction[ThrottleState] with LazyLogging {

  val awaitPullTimeout = 6.seconds

  override def run(state: ThrottleState,
                   forwardCopyOnAllLevels: Boolean)(implicit committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (state.terminate) {
      logger.debug(s"${state.name}: Ignoring wakeUp call. Compaction is terminated!")
      Future.unit
    } else if (state.running.compareAndSet(false, true)) {
      implicit val executionContext: ExecutionContext = state.executionContext

      Future
        .unit
        .flatMapUnit {
          state.wakeUp.set(false)

          runNow(
            state = state,
            forwardCopyOnAllLevels = forwardCopyOnAllLevels
          ) withCallback {
            state.running.set(false)

            if (state.wakeUp.get())
              run(
                state = state,
                forwardCopyOnAllLevels = forwardCopyOnAllLevels
              )
          }
        }
    } else {
      state.wakeUp.set(true)
      Future.unit
    }

  private[throttle] def runNow(state: ThrottleState,
                               forwardCopyOnAllLevels: Boolean)(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                                executionContext: ExecutionContext): Future[Unit] = {
    logger.debug(s"\n\n\n\n\n\n${state.name}: Running compaction now! forwardCopyOnAllLevels = $forwardCopyOnAllLevels!")

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
      state = state,
      currentJobs = jobs
    )
  }

  def shouldRun(level: LevelRef, newStateId: Long, state: ThrottleLevelState): Boolean =
    state match {
      case awaitingPull @ ThrottleLevelState.AwaitingJoin(_, timeout, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        awaitingPull.listenerInvoked || timeout.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        sleepDeadline.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())
    }

  private[throttle] def runJobs(state: ThrottleState,
                                currentJobs: Slice[LevelRef])(implicit committer: ActorWire[CompactionCommitter.type, Unit],
                                                              executionContext: ExecutionContext): Future[Unit] =
    if (state.terminate) {
      logger.warn(s"${state.name}: Cannot run jobs. Compaction is terminated.")
      Future.unit
    } else {
      logger.debug(s"${state.name}: Compaction order: ${currentJobs.map(_.levelNumber).mkString(", ")}")

      val level = currentJobs.headOrNull

      if (level == null) {
        logger.debug(s"${state.name}: Compaction round complete.") //all jobs complete.
        Future.unit
      } else {
        logger.debug(s"Level(${level.levelNumber}): ${state.name}: Running compaction.")
        val currentState = state.compactionStates.get(level)

        //Level's stateId should only be accessed here before compaction starts for the level.
        val stateId = level.stateId

        if ((currentState.isEmpty && level.nextCompactionDelay.fromNow.isOverdue()) || currentState.exists(state => shouldRun(level, stateId, state))) {
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")

          runJob(
            level = level,
            stateId = stateId
          ) flatMap {
            nextState =>
              logger.debug(s"Level(${level.levelNumber}): ${state.name}: next state $nextState.")
              state.compactionStates.put(
                key = level,
                value = nextState
              )

              runJobs(state, currentJobs.dropHead())
          }

        } else {
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: shouldRun = false.")
          runJobs(state, currentJobs.dropHead())
        }
      }
    }

  /**
   * It should be be re-fetched for the same compaction.
   */
  private[throttle] def runJob(level: LevelRef,
                               stateId: Long)(implicit ec: ExecutionContext,
                                              committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    level match {
      case zero: LevelZero =>
        pushForward(
          zero = zero,
          stateId = stateId
        )

      case level: Level =>
        pushForward(
          level = level,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    zero.nextLevel match {
      case Some(nextLevel: Level) =>
        pushForward(
          zero = zero,
          nextLevel = nextLevel,
          stateId = stateId
        )

      case None =>
        //no nextLevel, no compaction!
        ThrottleLevelState.Sleeping(
          sleepDeadline = ThrottleLevelState.longSleep,
          stateId = stateId
        ).toFuture
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    nextLevel: Level,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    zero.maps.last() match {
      case Some(map) =>
        logger.debug(s"Level(${zero.levelNumber}): Pushing LevelZero map :${map.pathOption} ")

        pushForward(
          zero = zero,
          nextLevel = nextLevel,
          stateId = stateId,
          map = map
        )

      case None =>
        logger.debug(s"Level(${zero.levelNumber}): NO LAST MAP. No more maps to merge.")

        ThrottleLevelState.Sleeping(
          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
          stateId = stateId
        ).toFuture
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    nextLevel: Level,
                                    stateId: Long,
                                    map: swaydb.core.map.Map[Slice[Byte], Memory, LevelZeroMapCache])(implicit ec: ExecutionContext,
                                                                                                      committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
  //    nextLevel.put(map = map) match {
  //      case Right(reserved @ LevelReservation.Reservation(mergeResultFuture, _)) =>
  //        logger.debug(s"Level(${zero.levelNumber}): Put to map successful.")
  //        mergeResultFuture
  //          .flatMap {
  //            mergeResult =>
  //              committer
  //                .ask
  //                .flatMap {
  //                  (impl, _) =>
  //                    impl.commit(
  //                      fromLevel = zero,
  //                      toLevel = nextLevel,
  //                      mergeResult = mergeResult
  //                    )
  //                }
  //                .mapUnit {
  //                  // If there is a failure removing the last map, maps will add the same map back into the queue and print
  //                  // error message to be handled by the User.
  //                  // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
  //                  // Maps are ALWAYS required to be processed sequentially in the order of write.
  //                  // Random order merging of maps should NOT be allowed.
  //                  zero.maps.removeLast(map) onLeftSideEffect {
  //                    error =>
  //                      val mapPath: String =
  //                        zero
  //                          .maps
  //                          .nextJob()
  //                          .map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")
  //
  //                      logger.error(
  //                        s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue." +
  //                          "No more maps will be pushed to Level1 until this error is fixed " +
  //                          "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
  //                          "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
  //                        error.exception
  //                      )
  //                  }
  //
  //                  ThrottleLevelState.Sleeping(
  //                    sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
  //                    stateId = stateId
  //                  )
  //                }
  //          }
  //          .withCallback(reserved.checkout())
  //
  //      case Right(failed: LevelReservation.Failed) =>
  //        failed.error match {
  //          case _ if zero.coreState.isNotRunning =>
  //            logger.debug(s"Level(${zero.levelNumber}): Failed to push due to shutdown.", failed.error.exception)
  //
  //          //do not log the stack if the IO.Left to merge was ContainsOverlappingBusySegments.
  //          case swaydb.Error.OverlappingPushSegment =>
  //            logger.debug(s"Level(${zero.levelNumber}): Failed to push", swaydb.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
  //
  //          case _ =>
  //            logger.error(s"Level(${zero.levelNumber}): Failed to push", failed.error.exception)
  //        }
  //
  //        ThrottleLevelState.Sleeping(
  //          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
  //          stateId = stateId
  //        ).toFuture
  //
  //      case Left(promise) =>
  //        logger.debug(s"Level(${zero.levelNumber}): Awaiting pull. stateId: $stateId.")
  //
  //        ThrottleLevelState.AwaitingPull(
  //          promise = promise,
  //          timeout = awaitPullTimeout.fromNow,
  //          stateId = stateId
  //        ).toFuture
  //    }
    ???

  private[throttle] def pushForward(level: Level,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter.type, Unit]): Future[ThrottleLevelState] =
    pushForward(
      level = level,
      stateId = stateId,
      segmentsToPush = level.nextThrottlePushCount max 1,
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

  private def pushForward(level: Level,
                          stateId: Long,
                          segmentsToPush: Int)(implicit ec: ExecutionContext,
                                               committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (level.isEmpty)
      Future.unit
    else
    //      runSegmentTask(CompactionSelector.select(level, segmentsToPush))
      ???

  private[throttle] def runSegmentTask(task: CompactionTask.Segments)(implicit executionContext: ExecutionContext,
                                                                      committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        runSegmentTask(task)

      case task: CompactionTask.CollapseSegments =>
        runSegmentTask(task)

      case task: CompactionTask.RefreshSegments =>
        runSegmentTask(task)

      case task: CompactionTask.CompactMaps =>
        runSegmentTask(task)

      case CompactionTask.Idle =>
        Future.unit
    }

  private[throttle] def runSegmentTask(task: CompactionTask.CompactSegments)(implicit executionContext: ExecutionContext,
                                                                             committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          task.targetLevel.merge(task.segments, removeDeletedRecords = false) map {
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
                  segments = task.tasks.flatMap(_.segments),
                  mergeResults = result
                )
            }
      }

  private[throttle] def runSegmentTask(task: CompactionTask.CollapseSegments)(implicit executionContext: ExecutionContext,
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
                  )
              }
        }

  private[throttle] def runSegmentTask(task: CompactionTask.RefreshSegments)(implicit executionContext: ExecutionContext,
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
                  )
              }
        }

  private[throttle] def runMapTask(task: CompactionTask.CompactMaps)(implicit executionContext: ExecutionContext,
                                                                     committer: ActorWire[CompactionCommitter.type, Unit]): Future[Unit] =
  //    if (task.maps.isEmpty)
  //      Future.unit
  //    else
  //      task
  //        .targetLevel
  //        .mergeMaps(task.maps)
  //        .flatMap {
  //          result =>
  //            committer
  //              .ask
  //              .flatMap {
  //                (impl, _) =>
  //                  impl.commit(
  //                    level = task.levelZero,
  //                    result = result
  //                  )
  //              }
  //        }
    ???

}

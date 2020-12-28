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
import swaydb.core.data.Memory
import swaydb.core.level._
import swaydb.core.level.compaction.Compaction
import swaydb.core.level.compaction.committer.CompactionCommitter
import swaydb.core.level.compaction.reception.LevelReservation
import swaydb.core.level.compaction.selector.{CollapseSegmentSelector, CompactSegmentSelector}
import swaydb.core.level.zero.{LevelZero, LevelZeroMapCache}
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice
import swaydb.data.util.Futures
import swaydb.data.util.Futures._
import swaydb.{ActorWire, IO}

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
                   forwardCopyOnAllLevels: Boolean)(implicit committer: ActorWire[CompactionCommitter, Unit]): Future[Unit] =
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
                               forwardCopyOnAllLevels: Boolean)(implicit committer: ActorWire[CompactionCommitter, Unit],
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
      case awaitingPull @ ThrottleLevelState.AwaitingPull(_, timeout, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        awaitingPull.listenerInvoked || timeout.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level(${level.levelNumber}): $state")
        sleepDeadline.isOverdue() || (newStateId != stateId && level.nextCompactionDelay.fromNow.isOverdue())
    }

  private[throttle] def runJobs(state: ThrottleState,
                                currentJobs: Slice[LevelRef])(implicit committer: ActorWire[CompactionCommitter, Unit],
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
                                              committer: ActorWire[CompactionCommitter, Unit]): Future[ThrottleLevelState] =
    level match {
      case zero: LevelZero =>
        pushForward(
          zero = zero,
          stateId = stateId
        )

      case level: NextLevel =>
        pushForward(
          level = level,
          stateId = stateId
        )

      case TrashLevel =>
        logger.error(s"Level(${level.levelNumber}):Received job for ${TrashLevel.getClass.getSimpleName}.")
        //trash Levels should are never submitted for compaction anyway. Give it a long delay.
        ThrottleLevelState.Sleeping(
          sleepDeadline = ThrottleLevelState.longSleep,
          stateId = stateId
        ).toFuture
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter, Unit]): Future[ThrottleLevelState] =
    zero.nextLevel match {
      case Some(nextLevel) =>
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
                                    nextLevel: NextLevel,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter, Unit]): Future[ThrottleLevelState] =
    zero.maps.nextJob() match {
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
                                    nextLevel: NextLevel,
                                    stateId: Long,
                                    map: swaydb.core.map.Map[Slice[Byte], Memory, LevelZeroMapCache])(implicit ec: ExecutionContext,
                                                                                                      committer: ActorWire[CompactionCommitter, Unit]): Future[ThrottleLevelState] =
    nextLevel.put(map = map) match {
      case Right(reserved @ LevelReservation.Reserved(mergeResultFuture, _)) =>
        logger.debug(s"Level(${zero.levelNumber}): Put to map successful.")
        mergeResultFuture
          .flatMap {
            mergeResult =>
              committer
                .ask
                .flatMap {
                  (impl, _) =>
                    impl.commit(
                      fromLevel = zero,
                      toLevel = nextLevel,
                      mergeResult = mergeResult
                    )
                }
                .mapUnit {
                  // If there is a failure removing the last map, maps will add the same map back into the queue and print
                  // error message to be handled by the User.
                  // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
                  // Maps are ALWAYS required to be processed sequentially in the order of write.
                  // Random order merging of maps should NOT be allowed.
                  zero.maps.removeLast(map) onLeftSideEffect {
                    error =>
                      val mapPath: String =
                        zero
                          .maps
                          .nextJob()
                          .map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")

                      logger.error(
                        s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue." +
                          "No more maps will be pushed to Level1 until this error is fixed " +
                          "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
                          "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
                        error.exception
                      )
                  }

                  ThrottleLevelState.Sleeping(
                    sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
                    stateId = stateId
                  )
                }
          }
          .withCallback(reserved.checkout())

      case Right(failed: LevelReservation.Failed) =>
        failed.error match {
          case _ if zero.coreState.isNotRunning =>
            logger.debug(s"Level(${zero.levelNumber}): Failed to push due to shutdown.", failed.error.exception)

          //do not log the stack if the IO.Left to merge was ContainsOverlappingBusySegments.
          case swaydb.Error.OverlappingPushSegment =>
            logger.debug(s"Level(${zero.levelNumber}): Failed to push", swaydb.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))

          case _ =>
            logger.error(s"Level(${zero.levelNumber}): Failed to push", failed.error.exception)
        }

        ThrottleLevelState.Sleeping(
          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
          stateId = stateId
        ).toFuture

      case Left(promise) =>
        logger.debug(s"Level(${zero.levelNumber}): Awaiting pull. stateId: $stateId.")

        ThrottleLevelState.AwaitingPull(
          promise = promise,
          timeout = awaitPullTimeout.fromNow,
          stateId = stateId
        ).toFuture
    }

  private[throttle] def pushForward(level: NextLevel,
                                    stateId: Long)(implicit ec: ExecutionContext,
                                                   committer: ActorWire[CompactionCommitter, Unit]): Future[ThrottleLevelState] =
    pushForward(
      level = level,
      stateId = stateId,
      segmentsToPush = level.nextThrottlePushCount max 1,
    ) flatMap {
      case Right(future) =>
        future mapUnit {
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

      case Left(promise) =>
        ThrottleLevelState.AwaitingPull(
          promise = promise,
          timeout = awaitPullTimeout.fromNow,
          stateId = stateId
        ).toFuture
    }

  private def pushForward(level: NextLevel,
                          stateId: Long,
                          segmentsToPush: Int)(implicit ec: ExecutionContext,
                                               committer: ActorWire[CompactionCommitter, Unit]): Future[Either[Promise[Unit], Future[Unit]]] =
    level.nextLevel match {
      case Some(nextLevel) =>
        val segmentsToMerge = CompactSegmentSelector.select(level, nextLevel, segmentsToPush)
        logger.debug(s"Level(${level.levelNumber}): mergeable: ${segmentsToMerge.size} ")

        Future {
          putForward(
            segments = segmentsToMerge,
            thisLevel = level,
            nextLevel = nextLevel
          )
        }

      case None =>
        runLastLevelCompaction(
          level = level,
          remainingCompactions = segmentsToPush
        )
    }

  def runLastLevelCompaction(level: NextLevel,
                             remainingCompactions: Int)(implicit ec: ExecutionContext,
                                                        committer: ActorWire[CompactionCommitter, Unit]): Future[Either[Promise[Unit], Future[Unit]]] = {
    logger.debug(s"Level(${level.levelNumber}): Last level compaction. remainingCompactions = $remainingCompactions.")
    if (level.hasNextLevel || remainingCompactions <= 0) {
      Futures.rightUnitFuture
    } else {
      runLastLevelExpirationCheck(
        level = level,
        remainingCompactions = remainingCompactions
      ) flatMap {
        case Right(future) =>
          future
            .flatMapUnit {
              runLastLevelCollapseCheck(
                level = level,
                remainingCompactions = remainingCompactions
              )
            }

        case Left(promise) =>
          Future.successful(Left(promise))
      }
    }
  }

  def runLastLevelExpirationCheck(level: NextLevel,
                                  remainingCompactions: Int)(implicit ec: ExecutionContext,
                                                             committer: ActorWire[CompactionCommitter, Unit]): Future[Either[Promise[Unit], Future[Unit]]] = {
    logger.debug(s"Level(${level.levelNumber}): Last level compaction. remainingCompactions = $remainingCompactions.")
    if (level.hasNextLevel || remainingCompactions <= 0)
      Futures.rightUnitFuture
    else
      Segment.getNearestDeadlineSegment(level.segments()) match {
        case segment: Segment if segment.nearestPutDeadline.exists(!_.hasTimeLeft()) =>
          level.refresh(segment) match {
            case scala.Right(reserved @ LevelReservation.Reserved(result, key)) =>
              logger.debug(s"Level(${level.levelNumber}): Refresh successful.")

              result match {
                case IO.Right(newSegments) =>
                  committer
                    .ask
                    .flatMap {
                      (impl, _) =>
                        impl.replace(
                          level = level,
                          old = segment,
                          neu = newSegments
                        ).withCallback(reserved.checkout())
                    }
                    .flatMapUnit(
                      runLastLevelExpirationCheck(
                        level = level,
                        remainingCompactions = remainingCompactions - 1
                      )
                    )

                case IO.Left(error) =>
                  logger.debug(s"Level(${level.levelNumber}): Failed to refresh", error.exception)
                  Futures.rightUnitFuture
              }

            case scala.Right(failed: LevelReservation.Failed) =>
              logger.debug(s"Level(${level.levelNumber}): Refresh failed.", failed.error.exception)
              Future.successful(Right(Future.failed(failed.error.exception)))

            case scala.Left(promise) =>
              logger.debug(s"Level(${level.levelNumber}): Later on refresh.")
              Future.successful(scala.Left(promise))
          }

        case Segment.Null | _: Segment =>
          logger.debug(s"Level(${level.levelNumber}): Check expired complete.")
          Futures.rightUnitFuture
      }
  }


  def runLastLevelCollapseCheck(level: NextLevel,
                                remainingCompactions: Int)(implicit ec: ExecutionContext,
                                                           committer: ActorWire[CompactionCommitter, Unit]): Future[Either[Promise[Unit], Future[Unit]]] = {
    logger.debug(s"Level(${level.levelNumber}): Last level compaction. remainingCompactions = $remainingCompactions.")
    if (level.hasNextLevel || remainingCompactions <= 0) {
      Futures.rightUnitFuture
    } else {
      logger.debug(s"Level(${level.levelNumber}): Collapse run.")
      level.collapse(segments = CollapseSegmentSelector.select(level, remainingCompactions max 2)) match { //need at least 2 for collapse.
        case scala.Right(reserved @ LevelReservation.Reserved(result, key)) =>
          result flatMap {
            case LevelCollapseResult.Empty =>
              Futures.rightUnitFuture

            case LevelCollapseResult.Collapsed(sourceSegments, mergeResult) =>
              logger.debug(s"Level(${level.levelNumber}): Collapsed ${sourceSegments.size} small segments.")
              committer.ask
                .flatMap {
                  (impl, _) =>
                    impl.replace(
                      level = level,
                      old = sourceSegments,
                      neu = mergeResult
                    ).withCallback(reserved.checkout())
                }
                .flatMapUnit {
                  runLastLevelCollapseCheck(
                    level = level,
                    remainingCompactions = remainingCompactions - 1
                  )
                }
          }

        case scala.Right(failed: LevelReservation.Failed) =>
          logger.debug(s"Level(${level.levelNumber}): Collpase failed.", failed.error.exception)
          Future.successful(Right(Future.failed(failed.error.exception)))

        case scala.Left(promise) =>
          logger.debug(s"Level(${level.levelNumber}): Later on collpase.")
          Future.successful(scala.Left(promise))
      }
    }
  }

  private[throttle] def putForward(segments: Iterable[Segment],
                                   thisLevel: NextLevel,
                                   nextLevel: NextLevel)(implicit executionContext: ExecutionContext,
                                                         committer: ActorWire[CompactionCommitter, Unit]): Either[Promise[Unit], Future[Unit]] =
    if (segments.isEmpty)
      Right(Future.unit)
    else
      nextLevel.put(segments = segments) map {
        case reserved @ LevelReservation.Reserved(mergeResultFuture, _) =>
          mergeResultFuture
            .flatMap {
              mergeResult =>
                committer.ask flatMap {
                  (impl, _) =>
                    impl.commit(
                      fromLevel = thisLevel,
                      segments = segments,
                      toLevel = nextLevel,
                      mergeResult = mergeResult
                    )
                }
            }
            .withCallback(reserved.checkout())

        case failed: LevelReservation.Failed =>
          Future.failed(failed.error.exception)
      }
}

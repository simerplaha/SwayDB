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
import swaydb.IO
import swaydb.core.data.Memory
import swaydb.core.level.compaction.Compaction
import swaydb.core.level.zero.{LevelZero, LevelZeroMapCache}
import swaydb.core.level.{LevelRef, NextLevel, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.data.compaction.ParallelMerge
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Promise}

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
                   forwardCopyOnAllLevels: Boolean): Unit =
    if (state.terminate)
      logger.debug(s"${state.name}: Ignoring wakeUp call. Compaction is terminated!")
    else
      runNow(
        state = state,
        forwardCopyOnAllLevels = forwardCopyOnAllLevels
      )

  private[throttle] def runNow(state: ThrottleState,
                               forwardCopyOnAllLevels: Boolean): Unit = {
    logger.debug(s"\n\n\n\n\n\n${state.name}: Running compaction now! forwardCopyOnAllLevels = $forwardCopyOnAllLevels!")
    if (forwardCopyOnAllLevels) {
      val totalCopies =
        copyForwardForEach(
          levels = state.levelsReversed,
          parallelMerge = state.parallelMerge
        )(state.executionContext)
      logger.debug(s"${state.name}: Copies $totalCopies compacted. Continuing compaction.")
    }

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

  @tailrec
  private[throttle] def runJobs(state: ThrottleState,
                                currentJobs: Slice[LevelRef]): Unit =
    if (state.terminate) {
      logger.warn(s"${state.name}: Cannot run jobs. Compaction is terminated.")
    } else {
      logger.debug(s"${state.name}: Compaction order: ${currentJobs.map(_.levelNumber).mkString(", ")}")

      val level = currentJobs.headOrNull

      if (level == null) {
        logger.debug(s"${state.name}: Compaction round complete.") //all jobs complete.
      } else {
        logger.debug(s"Level(${level.levelNumber}): ${state.name}: Running compaction.")
        val currentState = state.compactionStates.get(level)

        //Level's stateId should only be accessed here before compaction starts for the level.
        val stateId = level.stateId
        if ((currentState.isEmpty && level.nextCompactionDelay.fromNow.isOverdue()) || currentState.exists(state => shouldRun(level, stateId, state))) {
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")
          val nextState = runJob(level = level, stateId = stateId, parallelMerge = state.parallelMerge)(state.executionContext)
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: next state $nextState.")
          state.compactionStates.put(
            key = level,
            value = nextState
          )
          runJobs(state, currentJobs.dropHead())
        } else {
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: shouldRun = false.")
          runJobs(state, currentJobs.dropHead())
        }
      }
    }

  /**
   *
   *
   * It should be be re-fetched for the same compaction.
   */
  private[throttle] def runJob(level: LevelRef,
                               stateId: Long,
                               parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): ThrottleLevelState =
    level match {
      case zero: LevelZero =>
        pushForward(
          zero = zero,
          stateId = stateId,
          parallelMerge = parallelMerge
        )

      case level: NextLevel =>
        pushForward(
          level = level,
          stateId = stateId,
          parallelMerge = parallelMerge
        )

      case TrashLevel =>
        logger.error(s"Level(${level.levelNumber}):Received job for ${TrashLevel.getClass.getSimpleName}.")
        //trash Levels should are never submitted for compaction anyway. Give it a long delay.
        ThrottleLevelState.Sleeping(
          sleepDeadline = ThrottleLevelState.longSleep,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    stateId: Long,
                                    parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): ThrottleLevelState =
    zero.nextLevel match {
      case Some(nextLevel) =>
        pushForward(
          zero = zero,
          nextLevel = nextLevel,
          stateId = stateId,
          parallelMerge = parallelMerge
        )

      case None =>
        //no nextLevel, no compaction!
        ThrottleLevelState.Sleeping(
          sleepDeadline = ThrottleLevelState.longSleep,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    nextLevel: NextLevel,
                                    stateId: Long,
                                    parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): ThrottleLevelState =
    zero.maps.nextJob() match {
      case Some(map) =>
        logger.debug(s"Level(${zero.levelNumber}): Pushing LevelZero map :${map.pathOption} ")
        pushForward(
          zero = zero,
          nextLevel = nextLevel,
          stateId = stateId,
          map = map,
          parallelMerge = parallelMerge
        )

      case None =>
        logger.debug(s"Level(${zero.levelNumber}): NO LAST MAP. No more maps to merge.")
        ThrottleLevelState.Sleeping(
          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
          stateId = stateId
        )
    }

  private[throttle] def pushForward(zero: LevelZero,
                                    nextLevel: NextLevel,
                                    stateId: Long,
                                    map: swaydb.core.map.Map[Slice[Byte], Memory, LevelZeroMapCache],
                                    parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): ThrottleLevelState =
  //    nextLevel.put(
  //      map = map,
  //      parallelMerge = parallelMerge
  //    ) match {
  //      case IO.Right(IO.Right(_)) =>
  //        logger.debug(s"Level(${zero.levelNumber}): Put to map successful.")
  //        // If there is a failure removing the last map, maps will add the same map back into the queue and print
  //        // error message to be handled by the User.
  //        // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
  //        // Maps are ALWAYS required to be processed sequentially in the order of write.
  //        // Random order merging of maps should NOT be allowed.
  //        zero.maps.removeLast(map) onLeftSideEffect {
  //          error =>
  //            val mapPath: String =
  //              zero
  //                .maps
  //                .nextJob()
  //                .map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")
  //
  //            logger.error(
  //              s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue." +
  //                "No more maps will be pushed to Level1 until this error is fixed " +
  //                "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
  //                "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
  //              error.exception
  //            )
  //        }
  //
  //        ThrottleLevelState.Sleeping(
  //          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
  //          stateId = stateId
  //        )
  //
  //      case IO.Right(IO.Left(error)) =>
  //        error match {
  //          case _ if zero.coreState.isNotRunning =>
  //            logger.debug(s"Level(${zero.levelNumber}): Failed to push due to shutdown.", error.exception)
  //
  //          //do not log the stack if the IO.Left to merge was ContainsOverlappingBusySegments.
  //          case swaydb.Error.OverlappingPushSegment =>
  //            logger.debug(s"Level(${zero.levelNumber}): Failed to push", swaydb.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
  //
  //          case _ =>
  //            logger.error(s"Level(${zero.levelNumber}): Failed to push", error.exception)
  //        }
  //
  //        ThrottleLevelState.Sleeping(
  //          sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
  //          stateId = stateId
  //        )
  //
  //      case IO.Left(promise) =>
  //        logger.debug(s"Level(${zero.levelNumber}): Awaiting pull. stateId: $stateId.")
  //        ThrottleLevelState.AwaitingPull(
  //          promise = promise,
  //          timeout = awaitPullTimeout.fromNow,
  //          stateId = stateId
  //        )
  //    }
    ???

  private[throttle] def pushForward(level: NextLevel,
                                    stateId: Long,
                                    parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): ThrottleLevelState =
    pushForward(
      level = level,
      segmentsToPush = level.nextThrottlePushCount max 1,
      parallelMerge = parallelMerge
    ) match {
      case IO.Right(IO.Right(pushed)) =>
        logger.debug(s"Level(${level.levelNumber}): pushed $pushed Segments.")
        ThrottleLevelState.Sleeping(
          sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else level.nextCompactionDelay.fromNow,
          stateId = stateId
        )

      case IO.Right(IO.Left(_)) =>
        ThrottleLevelState.Sleeping(
          sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else (ThrottleLevelState.failureSleepDuration min level.nextCompactionDelay).fromNow,
          stateId = stateId
        )

      case IO.Left(promise) =>
        ThrottleLevelState.AwaitingPull(
          promise = promise,
          timeout = awaitPullTimeout.fromNow,
          stateId = stateId
        )
    }

  private def pushForward(level: NextLevel,
                          segmentsToPush: Int,
                          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Int]] =
    level.nextLevel match {
      case Some(nextLevel) =>
        val (copyable, mergeable) = level.optimalSegmentsPushForward(take = segmentsToPush)
        logger.debug(s"Level(${level.levelNumber}): copyable: ${copyable.size}, mergeable: ${mergeable.size} ")

        implicit val promise = IO.ExceptionHandler.PromiseUnit

        val segmentsToCopy =
          if (level.pushForwardStrategy.always) //copy all only if allowed by pushForwardStrategy
            copyable
          else
            copyable.take(segmentsToPush) //else copy enough to satisfy this compaction jobs requirements.

        putForward(
          segments = segmentsToCopy,
          thisLevel = level,
          nextLevel = nextLevel,
          parallelMerge = parallelMerge
        ) flatMap {
          case IO.Right(copied) =>
            if (copied >= segmentsToPush)
              IO.Right[Promise[Unit], IO[swaydb.Error.Level, Int]](IO.Right[swaydb.Error.Level, Int](copied))
            else
              putForward(
                segments = mergeable take segmentsToPush,
                thisLevel = level,
                nextLevel = nextLevel,
                parallelMerge = parallelMerge,
              ).transform(_.transform(_ + copied))

          case IO.Left(error) =>
            IO.Right(IO.Left(error))
        }

      case None =>
        IO.Right(
          runLastLevelCompaction(
            level = level,
            checkExpired = true,
            remainingCompactions = segmentsToPush,
            segmentsCompacted = 0,
            parallelMerge = parallelMerge
          )
        )(IO.ExceptionHandler.PromiseUnit)
    }

  //  @tailrec
  def runLastLevelCompaction(level: NextLevel,
                             checkExpired: Boolean,
                             remainingCompactions: Int,
                             segmentsCompacted: Int,
                             parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Int] = {
    //    logger.debug(s"Level(${level.levelNumber}): Last level compaction. checkExpired = $checkExpired. remainingCompactions = $remainingCompactions. segmentsCompacted = $segmentsCompacted.")
    //    if (level.hasNextLevel || remainingCompactions <= 0) {
    //      IO.Right[swaydb.Error.Level, Int](segmentsCompacted)
    //    } else if (checkExpired) {
    //      logger.debug(s"Level(${level.levelNumber}): checking expired.")
    //      Segment.getNearestDeadlineSegment(level.segmentsInLevel()) match {
    //        case segment: Segment if segment.nearestPutDeadline.exists(!_.hasTimeLeft()) =>
    //          level.refresh(segment) match {
    //            case IO.Right(IO.Right(_)) =>
    //              logger.debug(s"Level(${level.levelNumber}): Refresh successful.")
    //              runLastLevelCompaction(
    //                level = level,
    //                checkExpired = checkExpired,
    //                remainingCompactions = remainingCompactions - 1,
    //                segmentsCompacted = segmentsCompacted + 1,
    //                parallelMerge = parallelMerge
    //              )
    //
    //            case IO.Left(_) =>
    //              logger.debug(s"Level(${level.levelNumber}): Later on refresh.")
    //              runLastLevelCompaction(
    //                level = level,
    //                checkExpired = false,
    //                remainingCompactions = remainingCompactions,
    //                segmentsCompacted = segmentsCompacted,
    //                parallelMerge = parallelMerge
    //              )
    //
    //            case IO.Right(IO.Left(_)) =>
    //              logger.debug(s"Level(${level.levelNumber}): Later on refresh 2.")
    //              runLastLevelCompaction(
    //                level = level,
    //                checkExpired = false,
    //                remainingCompactions = remainingCompactions,
    //                segmentsCompacted = segmentsCompacted,
    //                parallelMerge = parallelMerge
    //              )
    //          }
    //
    //        case Segment.Null | _: Segment =>
    //          logger.debug(s"Level(${level.levelNumber}): Check expired complete.")
    //          runLastLevelCompaction(
    //            level = level,
    //            checkExpired = false,
    //            remainingCompactions = remainingCompactions,
    //            segmentsCompacted = segmentsCompacted,
    //            parallelMerge = parallelMerge
    //          )
    //      }
    //    } else {
    //      logger.debug(s"Level(${level.levelNumber}): Collapse run.")
    //      level.collapse(
    //        segments = level.optimalSegmentsToCollapse(remainingCompactions max 2),
    //        parallelMerge = parallelMerge
    //      ) match { //need at least 2 for collapse.
    //        case IO.Right(IO.Right(count)) =>
    //          logger.debug(s"Level(${level.levelNumber}): Collapsed $count small segments.")
    //          runLastLevelCompaction(
    //            level = level,
    //            checkExpired = checkExpired,
    //            remainingCompactions = if (count == 0) 0 else remainingCompactions - count,
    //            segmentsCompacted = segmentsCompacted + count,
    //            parallelMerge = parallelMerge
    //          )
    //
    //        case IO.Left(_) =>
    //          logger.debug(s"Level(${level.levelNumber}): Later on collapse.")
    //          runLastLevelCompaction(
    //            level = level,
    //            checkExpired = checkExpired,
    //            remainingCompactions = 0,
    //            segmentsCompacted = segmentsCompacted,
    //            parallelMerge = parallelMerge
    //          )
    //
    //        case IO.Right(IO.Left(_)) =>
    //          logger.debug(s"Level(${level.levelNumber}): Later on collapse 2.")
    //          runLastLevelCompaction(
    //            level = level,
    //            checkExpired = checkExpired,
    //            remainingCompactions = 0,
    //            segmentsCompacted = segmentsCompacted,
    //            parallelMerge = parallelMerge
    //          )
    //      }
    //    }
    ???
  }

  /**
   * Runs lazy error checks. Ignores all errors and continues copying
   * each Level starting from the lowest level first.
   */
  private[throttle] def copyForwardForEach(levels: Slice[LevelRef],
                                           parallelMerge: ParallelMerge)(implicit executionContext: ExecutionContext): Int =
    levels.foldLeft(0) {
      case (totalCopies, level: NextLevel) =>
        if (level.pushForwardStrategy.always) {
          val copied = copyForward(level = level, parallelMerge = parallelMerge)
          logger.debug(s"Level(${level.levelNumber}): Compaction copied $copied.")
          totalCopies + copied
        } else {
          totalCopies
        }

      case (copies, TrashLevel | _: LevelZero) =>
        copies
    }

  private def copyForward(level: NextLevel,
                          parallelMerge: ParallelMerge)(implicit executionContext: ExecutionContext): Int =
    level.nextLevel match {
      case Some(nextLevel) =>
        val segmentsInLevel = level.segmentsInLevel()
        val (copyable, nonCopyable) = nextLevel.partitionCopyable(segmentsInLevel)
        logger.debug(s"Level(${level.levelNumber}): Total segments: ${segmentsInLevel.size}, Can copy: ${copyable.size} segments. Remaining: ${nonCopyable.size} segments.")
        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel,
          parallelMerge = parallelMerge
        ) match {
          case IO.Right(IO.Right(copied)) =>
            logger.debug(s"Level(${level.levelNumber}): Forward copied $copied Segments.")
            copied

          case IO.Right(IO.Left(error)) =>
            logger.error(s"Level(${level.levelNumber}): Failed copy Segments forward.", error.exception)
            0

          case IO.Left(_) =>
            //this should never really occur when no other concurrent compactions are occurring.
            logger.warn(s"Level(${level.levelNumber}): Received later compaction.")
            0
        }

      case None =>
        0
    }

  private[throttle] def putForward(segments: Iterable[Segment],
                                   thisLevel: NextLevel,
                                   nextLevel: NextLevel,
                                   parallelMerge: ParallelMerge)(implicit executionContext: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Int]] =
  //    if (segments.isEmpty)
  //      IO.zeroZero
  //    else
  //      nextLevel.put(segments = segments, parallelMerge = parallelMerge) map {
  //        case IO.Right(_) =>
  //          thisLevel
  //            .removeSegments(segments)
  //            //transform because remove might be eventual depending on the level's config.
  //            .transform(_ => segments.size)
  //            .recover {
  //              case _ =>
  //                segments.size
  //            }
  //
  //        case IO.Left(error) =>
  //          IO.Left(error)
  //      }
    ???
}

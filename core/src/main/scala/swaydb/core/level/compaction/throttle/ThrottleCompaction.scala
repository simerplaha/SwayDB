/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.data.Memory
import swaydb.core.level.compaction.Compaction
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice
import scala.concurrent.duration._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Promise}

/**
 * This object does not implement any concurrency which should be handled by an Actor.
 *
 * It just implements functions that given a Level and it's [[ThrottleState]] mutates the state
 * such to reflect it's current compaction state. This state can then used to determine
 * how the next compaction should occur.
 *
 * State mutation is necessary to avoid unnecessary garbage during compaction. Functions returning Unit mutate the state.
 */
private[core] object ThrottleCompaction extends Compaction[ThrottleState] with LazyLogging {

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

  private[compaction] def runNow(state: ThrottleState,
                                 forwardCopyOnAllLevels: Boolean): Unit = {
    logger.debug(s"${state.name}: Running compaction now! forwardCopyOnAllLevels = $forwardCopyOnAllLevels!")
    if (forwardCopyOnAllLevels) {
      val totalCopies = copyForwardForEach(state.levelsReversed)(state.executionContext)
      logger.debug(s"${state.name}: Copies $totalCopies compacted. Continuing compaction.")
    }

    //run compaction jobs
    runJobs(
      state = state,
      currentJobs = state.levels.sorted(state.ordering)
    )
  }

  def shouldRun(levelNumber: Long, newStateId: Long, state: ThrottleLevelState): Boolean =
    state match {
      case awaitingPull @ ThrottleLevelState.AwaitingPull(_, timeout, stateId) =>
        logger.debug(s"Level($levelNumber): $state")
        awaitingPull.listenerInvoked || timeout.isOverdue() || newStateId != stateId

      case ThrottleLevelState.Sleeping(sleepDeadline, stateId) =>
        logger.debug(s"Level($levelNumber): $state")
        sleepDeadline.isOverdue() || newStateId != stateId
    }

  @tailrec
  private[compaction] def runJobs(state: ThrottleState,
                                  currentJobs: Slice[LevelRef]): Unit =
    if (state.terminate)
      logger.warn(s"${state.name}: Cannot run jobs. Compaction is terminated.")
    else
      currentJobs.headOption match {
        case Some(level) =>
          logger.debug(s"Level(${level.levelNumber}): ${state.name}: Running compaction.")
          val currentState = state.compactionStates.get(level)
          //Level's stateId should only be accessed here before compaction starts for the level.
          val stateId = level.stateId
          if (currentState.forall(state => shouldRun(level.levelNumber, stateId, state))) {
            logger.debug(s"Level(${level.levelNumber}): ${state.name}: ${if (currentState.isEmpty) "Initial run" else "shouldRun = true"}.")
            val nextState = runJob(level, stateId)(state.executionContext)
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

        case None =>
          //all jobs complete.
          logger.debug(s"${state.name}: Compaction round complete.")
      }

  /**
   *
   *
   * It should be be re-fetched for the same compaction.
   */
  private[compaction] def runJob(level: LevelRef, stateId: Long)(implicit ec: ExecutionContext): ThrottleLevelState =
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
        )
    }

  private[compaction] def pushForward(zero: LevelZero, stateId: Long)(implicit ec: ExecutionContext): ThrottleLevelState =
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
        )
    }

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel,
                                      stateId: Long)(implicit ec: ExecutionContext): ThrottleLevelState =
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
          sleepDeadline = zero.nextCompactionDelay.fromNow,
          stateId = stateId
        )
    }

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel,
                                      stateId: Long,
                                      map: swaydb.core.map.Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): ThrottleLevelState =
    nextLevel.put(map) match {
      case IO.Right(IO.Right(_)) =>
        logger.debug(s"Level(${zero.levelNumber}): Put to map successful.")
        // If there is a failure removing the last map, maps will add the same map back into the queue and print
        // error message to be handled by the User.
        // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
        // Maps are ALWAYS required to be processed sequentially in the order of write.
        // Un-order merging of maps should NOT be allowed.
        zero.maps.removeLast() foreach {
          result =>
            result onLeftSideEffect {
              error =>
                val mapPath: String =
                  zero
                    .maps
                    .last()
                    .map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")

                logger.error(
                  s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue." +
                    "No more maps will be pushed to Level1 until this error is fixed " +
                    "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
                    "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
                  error.exception
                )
            }
        }

        ThrottleLevelState.Sleeping(
          sleepDeadline = zero.nextCompactionDelay.fromNow,
          stateId = stateId
        )

      case IO.Right(IO.Left(error)) =>
        error match {
          //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
          case swaydb.Error.OverlappingPushSegment =>
            logger.debug(s"Level(${zero.levelNumber}): Failed to push", swaydb.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
          case _ =>
            logger.error(s"Level(${zero.levelNumber}): Failed to push", error.exception)
        }

        ThrottleLevelState.Sleeping(
          sleepDeadline = ThrottleLevelState.failureSleepDuration.fromNow,
          stateId = stateId
        )

      case IO.Left(promise) =>
        ThrottleLevelState.AwaitingPull(
          promise = promise,
          timeout = awaitPullTimeout.fromNow,
          stateId = stateId
        )
    }

  private[compaction] def pushForward(level: NextLevel, stateId: Long)(implicit ec: ExecutionContext): ThrottleLevelState =
    pushForward(level, level.nextThrottlePushCount max 1) match {
      case IO.Right(IO.Right(pushed)) =>
        logger.debug(s"Level(${level.levelNumber}): pushed $pushed Segments.")
        ThrottleLevelState.Sleeping(
          sleepDeadline = level.nextCompactionDelay.fromNow,
          stateId = stateId
        )

      case IO.Right(IO.Left(_)) =>
        ThrottleLevelState.Sleeping(
          sleepDeadline = (ThrottleLevelState.failureSleepDuration min level.nextCompactionDelay).fromNow,
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
                          segmentsToPush: Int)(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Int]] =
    level.nextLevel match {
      case Some(nextLevel) =>
        val (copyable, mergeable) = level.optimalSegmentsPushForward(take = segmentsToPush)
        logger.debug(s"Level(${level.levelNumber}): copyable: ${copyable.size}, mergeable: ${mergeable.size} ")

        implicit val promise = IO.ExceptionHandler.PromiseUnit

        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel
        ) flatMap {
          case IO.Right(copied) =>
            if (copied >= segmentsToPush)
              IO.Right[Promise[Unit], IO[swaydb.Error.Level, Int]](IO.Right[swaydb.Error.Level, Int](copied))
            else
              putForward(
                segments = mergeable take segmentsToPush,
                thisLevel = level,
                nextLevel = nextLevel
              ).map(_.map(_ + copied))

          case IO.Left(error) =>
            IO.Right(IO.Left(error))
        }

      case None =>
        IO.Right(
          runLastLevelCompaction(
            level = level,
            checkExpired = true,
            remainingCompactions = segmentsToPush,
            segmentsCompacted = 0
          )
        )(IO.ExceptionHandler.PromiseUnit)
    }

  @tailrec
  def runLastLevelCompaction(level: NextLevel,
                             checkExpired: Boolean,
                             remainingCompactions: Int,
                             segmentsCompacted: Int)(implicit ec: ExecutionContext): IO[swaydb.Error.Level, Int] =
    if (level.hasNextLevel || remainingCompactions <= 0)
      IO.Right[swaydb.Error.Level, Int](segmentsCompacted)
    else if (checkExpired)
      Segment.getNearestDeadlineSegment(level.segmentsInLevel()) match {
        case Some(segment) =>
          level.refresh(segment) match {
            case IO.Right(IO.Right(_)) =>
              logger.debug(s"Level(${level.levelNumber}): Refresh successful.")
              runLastLevelCompaction(
                level = level,
                checkExpired = checkExpired,
                remainingCompactions = remainingCompactions - 1,
                segmentsCompacted = segmentsCompacted + 1
              )

            case IO.Left(_) =>
              logger.debug(s"Level(${level.levelNumber}): Later on refresh.")
              runLastLevelCompaction(
                level = level,
                checkExpired = false,
                remainingCompactions = remainingCompactions,
                segmentsCompacted = segmentsCompacted
              )

            case IO.Right(IO.Left(_)) =>
              runLastLevelCompaction(
                level = level,
                checkExpired = false,
                remainingCompactions = remainingCompactions,
                segmentsCompacted = segmentsCompacted
              )
          }

        case None =>
          runLastLevelCompaction(
            level = level,
            checkExpired = false,
            remainingCompactions = remainingCompactions,
            segmentsCompacted = segmentsCompacted
          )
      }
    else
      level.collapse(level.optimalSegmentsToCollapse(remainingCompactions max 2)) match { //need at least 2 for collapse.
        case IO.Right(IO.Right(count)) =>
          logger.debug(s"Level(${level.levelNumber}): Collapsed $count small segments.")
          runLastLevelCompaction(
            level = level,
            checkExpired = checkExpired,
            remainingCompactions = if (count == 0) 0 else remainingCompactions - count,
            segmentsCompacted = segmentsCompacted + count
          )

        case IO.Left(_) =>
          logger.debug(s"Level(${level.levelNumber}): Later on collapse.")
          runLastLevelCompaction(
            level = level,
            checkExpired = checkExpired,
            remainingCompactions = 0,
            segmentsCompacted = segmentsCompacted
          )

        case IO.Right(IO.Left(_)) =>
          runLastLevelCompaction(
            level = level,
            checkExpired = checkExpired,
            remainingCompactions = 0,
            segmentsCompacted = segmentsCompacted
          )
      }

  /**
   * Runs lazy error checks. Ignores all errors and continues copying
   * each Level starting from the lowest level first.
   */
  private[compaction] def copyForwardForEach(levels: Slice[LevelRef])(implicit ec: ExecutionContext): Int =
    levels.foldLeft(0) {
      case (totalCopies, level: NextLevel) =>
        val copied = copyForward(level)
        logger.debug(s"Level(${level.levelNumber}): Compaction copied $copied. Starting compaction!")
        totalCopies + copied

      case (copies, TrashLevel | _: LevelZero) =>
        copies
    }

  private def copyForward(level: NextLevel)(implicit ec: ExecutionContext): Int =
    level.nextLevel map {
      nextLevel =>
        val (copyable, _) = nextLevel.partitionUnreservedCopyable(level.segmentsInLevel())
        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel
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
    } getOrElse 0

  private[compaction] def putForward(segments: Iterable[Segment],
                                     thisLevel: NextLevel,
                                     nextLevel: NextLevel)(implicit ec: ExecutionContext): IO[Promise[Unit], IO[swaydb.Error.Level, Int]] =
    if (segments.isEmpty)
      IO.zeroZero
    else
      nextLevel.put(segments) map {
        case IO.Right(_) =>
          thisLevel
            .removeSegments(segments)
            .recoverWith {
              case _ =>
                IO.Right[swaydb.Error.Level, Int](segments.size)
            }

        case IO.Left(error) =>
          IO.Left(error)
      }
}

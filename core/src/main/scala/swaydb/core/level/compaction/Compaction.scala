package swaydb.core.level.compaction

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.concurrent.duration._

/**
  * This object does not implement any concurrency which should be handled by an Actor.
  *
  * It just implements functions that given a Level and it's [[CompactionGroupState]] mutates the state
  * such to reflect it's current compaction state. This state can then used to determine
  * how the next compaction should occur.
  *
  * State mutation is necessary to avoid unnecessary garbage during compaction. Functions returning Unit mutate the state.
  */
private[level] object Compaction extends LazyLogging {

  val awaitPullTimeout = 30.seconds.fromNow

  def run(state: CompactionGroupState,
          forwardCopyOnAllLevels: Boolean): Unit =
    if (state.terminate)
      logger.debug("Ignoring wakeUp call. Compaction is terminated!")
    else
      runNow(
        state = state,
        forwardCopyOnAllLevels = forwardCopyOnAllLevels
      )

  private[compaction] def runNow(state: CompactionGroupState,
                                 forwardCopyOnAllLevels: Boolean): Unit = {
    if (forwardCopyOnAllLevels) {
      val totalCopies = copyForwardForEach(state.levelsReversed)
      logger.debug(s"Compaction copied $totalCopies. Starting compaction!")
    }
    //run compaction jobs
    runJobs(
      state = state,
      currentJobs = state.levels.sorted(state.ordering)
    )
  }

  def shouldRun(level: LevelRef, state: LevelCompactionState): Boolean =
    state match {
      case waiting @ LevelCompactionState.AwaitingPull(_, timeout, previousStateID) =>
        waiting.isReady || timeout.isOverdue() || level.stateID != previousStateID

      case LevelCompactionState.Sleep(sleepDeadline, previousStateID) =>
        sleepDeadline.isOverdue() || level.stateID != previousStateID
    }

  @tailrec
  private[compaction] def runJobs(state: CompactionGroupState,
                                  currentJobs: Slice[LevelRef]): Unit =
    if (state.terminate)
      logger.warn("Cannot run jobs. Compaction is terminated.")
    else
      currentJobs.headOption match {
        //project next job
        case Some(level) =>
          if (state.compactionStates.get(level).forall(state => shouldRun(level, state)))
            state.compactionStates.put(
              key = level,
              value = runJob(level)
            )
          else
            runJobs(state, currentJobs.dropHead())

        case None =>
          () //all jobs complete.
      }

  private[compaction] def runJob(level: LevelRef): LevelCompactionState =
    level match {
      case zero: LevelZero =>
        pushForward(zero = zero)

      case level: NextLevel =>
        pushForward(level = level)

      case TrashLevel =>
        logger.error(s"Received job for ${TrashLevel.getClass.getSimpleName}.")
        LevelCompactionState.longSleep(level.stateID)
    }

  @tailrec
  private[compaction] def pushForward(level: NextLevel): LevelCompactionState = {
    val throttle = level.throttle(level.meter)
    if (throttle.pushDelay.fromNow.isOverdue())
      pushForward(level, throttle.segmentsToPush max 1) match {
        case IO.Success(_) =>
          pushForward(level)

        case later @ IO.Later(_, _) =>
          LevelCompactionState.AwaitingPull(
            later = later,
            timeout = awaitPullTimeout,
            previousStateID = level.stateID
          )

        case IO.Failure(_) =>
          LevelCompactionState.Sleep(
            sleepDeadline = LevelCompactionState.failureSleepDuration,
            previousStateID = level.stateID
          )
      }
    else
      LevelCompactionState.Sleep(
        sleepDeadline = throttle.pushDelay.fromNow,
        previousStateID = level.stateID
      )
  }

  private[compaction] def pushForward(zero: LevelZero): LevelCompactionState =
    zero.nextLevel map {
      nextLevel =>
        pushForward(
          zero,
          nextLevel
        )
    } getOrElse LevelCompactionState.longSleep(zero.stateID)

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel): LevelCompactionState =
    zero.maps.last() match {
      case Some(map) =>
        logger.debug(s"{}: Pushing LevelZero map {}", zero.path, map.pathOption)
        pushForward(
          zero = zero,
          nextLevel = nextLevel,
          map = map
        )

      case None =>
        logger.debug(s"{}: NO LAST MAP. No more maps to merge.", zero.path)
        LevelCompactionState.longSleep(zero.stateID)
    }

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel,
                                      map: swaydb.core.map.Map[Slice[Byte], Memory.SegmentResponse]): LevelCompactionState =
    nextLevel.put(map) match {
      case IO.Success(_) =>
        logger.debug(s"{}: Put to map successful.", zero.path)
        // If there is a failure removing the last map, maps will add the same map back into the queue and print
        // error message to be handled by the User.
        // Do not trigger another Push. This will stop LevelZero from pushing new memory maps to Level1.
        // Maps are ALWAYS required to be processed sequentially in the order of write.
        // Un order merging of maps should NOT be allowed.
        zero.maps.removeLast() foreach {
          result =>
            result onFailureSideEffect {
              error =>
                val mapPath: String = zero.maps.last().map(_.pathOption.map(_.toString).getOrElse("No path")).getOrElse("No map")
                logger.error(
                  s"Failed to delete the oldest memory map '$mapPath'. The map is added back to the memory-maps queue." +
                    "No more maps will be pushed to Level1 until this error is fixed " +
                    "as sequential conversion of memory-map files to Segments is required to maintain data accuracy. " +
                    "Please check file system permissions and ensure that SwayDB can delete files and reboot the database.",
                  error.exception
                )
            }
        }
        LevelCompactionState.longSleep(zero.stateID)

      case IO.Failure(exception) =>
        exception match {
          //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
          case IO.Error.OverlappingPushSegment =>
            logger.debug(s"{}: Failed to push", zero.path, IO.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
          case _ =>
            logger.error(s"{}: Failed to push", zero.path, exception)
        }
        LevelCompactionState.longSleep(zero.stateID)

      case later @ IO.Later(_, _) =>
        LevelCompactionState.AwaitingPull(
          later = later,
          timeout = awaitPullTimeout,
          previousStateID = zero.stateID
        )
    }

  private[compaction] def pushForward(level: NextLevel,
                                      segmentsToPush: Int): IO.Async[Int] =
    level.nextLevel map {
      nextLevel =>
        val (copyable, mergeable) = nextLevel.partitionUnreservedCopyable(level.segmentsInLevel())
        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel
        ) flatMap {
          copied =>
            if (segmentsToPush >= copied)
              IO.Success(copied)
            else
              putForward(
                segments = mergeable,
                thisLevel = level,
                nextLevel = nextLevel
              )
        }
    } getOrElse {
      runLastLevelCompaction(
        level = level,
        checkExpired = true,
        remainingCompactions = segmentsToPush,
        segmentsCompacted = 0
      ).asAsync
    }

  @tailrec
  def runLastLevelCompaction(level: NextLevel,
                             checkExpired: Boolean,
                             remainingCompactions: Int,
                             segmentsCompacted: Int): IO[Int] =
    if (!level.hasNextLevel || remainingCompactions == 0)
      IO.Success(segmentsCompacted)
    else if (checkExpired)
      Segment.getNearestDeadlineSegment(level.segmentsInLevel()) match {
        case Some(segment) =>
          level.refresh(segment) match {
            case IO.Success(_) =>
              runLastLevelCompaction(
                level = level,
                checkExpired = checkExpired,
                remainingCompactions = remainingCompactions - 1,
                segmentsCompacted = segmentsCompacted + 1
              )

            case IO.Later(_, _) =>
              runLastLevelCompaction(
                level = level,
                checkExpired = false,
                remainingCompactions = remainingCompactions,
                segmentsCompacted = segmentsCompacted
              )

            case IO.Failure(_) =>
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
      level.collapse(level.takeSmallSegments(remainingCompactions max 2)) match { //need at least 2 for collapse.
        case IO.Success(count) =>
          runLastLevelCompaction(
            level = level,
            checkExpired = false,
            remainingCompactions = 0,
            segmentsCompacted = segmentsCompacted + count
          )

        case IO.Later(_, _) | IO.Failure(_) =>
          runLastLevelCompaction(
            level = level,
            checkExpired = false,
            remainingCompactions = 0,
            segmentsCompacted = segmentsCompacted
          )
      }

  /**
    * Runs lazy error checks. Ignores all errors and continues copying
    * each Level starting from the lowest level first.
    */
  private[compaction] def copyForwardForEach(levels: Slice[LevelRef]): Int =
    levels.foldLeft(0) {
      case (totalCopies, level: NextLevel) =>
        val copied = copyForward(level)
        logger.debug(s"Compaction copied $copied. Starting compaction!")
        totalCopies + copied

      case (copies, TrashLevel | _: LevelZero) =>
        copies
    }

  private def copyForward(level: NextLevel): Int =
    level.nextLevel map {
      nextLevel =>
        val (copyable, _) = nextLevel.partitionUnreservedCopyable(level.segmentsInLevel())
        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel
        ) match {
          case IO.Success(copied) =>
            logger.debug(s"Forward copied $copied Segments for level: ${level.rootPath}.")
            copied

          case IO.Failure(error) =>
            logger.error(s"Failed copy Segments forward for level: ${level.rootPath}", error.exception)
            0

          case IO.Later(_, _) =>
            //this should never really occur when no other concurrent compactions are occurring.
            logger.warn(s"Received later compaction for level: ${level.rootPath}.")
            0
        }
    } getOrElse 0

  private[compaction] def putForward(segments: Iterable[Segment],
                                     thisLevel: NextLevel,
                                     nextLevel: NextLevel): IO.Async[Int] =
    if (segments.isEmpty)
      IO.zero
    else
      nextLevel.put(segments) match {
        case IO.Success(_) =>
          thisLevel.removeSegments(segments) recoverWith {
            case _ =>
              IO.Success(segments.size)
          } asAsync

        case async @ IO.Later(_, _) =>
          async mapAsync (_ => 0)

        case IO.Failure(error) =>
          IO.Failure(error)
      }
}

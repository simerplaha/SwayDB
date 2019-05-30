package swaydb.core.level.compaction

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.core.util.Delay
import swaydb.data.IO
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

/**
  * Implements functions to execution Compaction on [[CompactionState]].
  * Once executed Compaction will run infinitely until shutdown.
  *
  * The speed of compaction depends on [[swaydb.core.level.Level.throttle]] which
  * is used to determine how overflow is a Level and how often should compaction run.
  */


private[level] object Compaction extends CompactionStrategy[CompactionState] with LazyLogging {

  val awaitPullTimeout = 30.seconds.fromNow

  def copyAndStart(state: CompactionState)(implicit ordering: Ordering[LevelRef],
                                           ec: ExecutionContext): IO[Unit] =
    IO(wakeUp(state = state, forwardCopyOnAllLevels = true))

  def start(state: CompactionState)(implicit ordering: Ordering[LevelRef],
                                    ec: ExecutionContext): IO[Unit] =
    IO(wakeUp(state = state, forwardCopyOnAllLevels = false))

  def zeroReady(state: CompactionState)(implicit ordering: Ordering[LevelRef],
                                        ec: ExecutionContext): Unit = {
    state.zeroReady.compareAndSet(false, true)
    println(s"Zero ready. Already running: ${state.running}")
    wakeUp(state = state, forwardCopyOnAllLevels = false)
  }

  def terminate(state: CompactionState): Unit =
    state.terminate = true

  private def copyForwardLazily(state: CompactionState): IO[Unit] =
    state.zero.nextLevel map {
      case level: NextLevel =>
        val totalCopies = copyForwardOnAllLevels(level)
        logger.debug(s"Compaction copied $totalCopies. Starting compaction!")
        IO.unit

      case TrashLevel =>
        IO.unit
    } getOrElse IO.unit

  /**
    * Mutates current state. Do not create copies for memory sake.
    */
  private[compaction] def resetState(state: CompactionState): CompactionState = {
    state.sleepTask foreach (_.cancel())
    state.sleepTask = None
    state
  }

  private[compaction] def wakeUp(state: CompactionState,
                                 forwardCopyOnAllLevels: Boolean)(implicit ordering: Ordering[LevelRef],
                                                                  ec: ExecutionContext): Unit =
    if (!state.terminate && state.running.compareAndSet(false, true)) {
      if (forwardCopyOnAllLevels) {
        val totalCopies = copyForwardLazily(state)
        logger.debug(s"Compaction copied $totalCopies. Starting compaction!")
      }
      val newState = resetState(state)
      runJobs(
        state = newState,
        currentJobs = newState.levels.sorted,
        iterationNumber = 1,
        sleep = None
      )
    }

  private[compaction] def sleep(state: CompactionState,
                                duration: FiniteDuration)(implicit ordering: Ordering[LevelRef],
                                                          ec: ExecutionContext) =
    if (!state.terminate && state.running.compareAndSet(true, false)) {
      val task = Delay.task(duration)(wakeUp(state, forwardCopyOnAllLevels = true))
      state.sleepTask = Some(task)
    }

  def shouldRun(state: LevelCompactionState): Boolean =
    state match {
      case LevelCompactionState.AwaitingPull(ready, timeout) =>
        ready || timeout.isOverdue()

      case LevelCompactionState.Sleeping(ready) =>
        ready.fromNow.isOverdue()

      case LevelCompactionState.Idle | LevelCompactionState.Failed =>
        true
    }

  /**
    * Should only be executed by [[wakeUp]] or by itself.
    */
  @tailrec
  private[compaction] def runJobs(state: CompactionState,
                                  currentJobs: List[LevelRef],
                                  iterationNumber: Int,
                                  sleep: Option[FiniteDuration])(implicit ordering: Ordering[LevelRef],
                                                                 ec: ExecutionContext): Unit =
    if (state.terminate)
      ()
    else
      currentJobs.headOption match {
        case Some(level) =>
          //if there is a wake up call from LevelZero then re-prioritise Levels and run compaction.
          if (state.zeroReady.compareAndSet(true, false)) {
            runJobs(state, state.levels.sorted, 1, sleep)
          } else if (shouldRun(state.compactionStates.getOrDefault(level, LevelCompactionState.Idle))) {
            val newState = runJob(level)
            newState match {
              case LevelCompactionState.AwaitingPull(ready, timeout) =>
                if (ready || timeout.isOverdue()) {
                  runJobs(state, currentJobs, iterationNumber + 1, sleep)
                } else {
                  val sleepFor = sleep.map(_ min timeout.timeLeft).orElse(Some(timeout.timeLeft))
                  state.compactionStates.put(level, newState)
                  runJobs(state, currentJobs.drop(1), iterationNumber + 1, sleepFor)
                }

              case LevelCompactionState.Idle | LevelCompactionState.Failed =>
                state.compactionStates.put(level, newState)
                runJobs(state, currentJobs.drop(1), iterationNumber + 1, sleep)

              case LevelCompactionState.Sleeping(duration) =>
                val sleepFor = sleep.map(_ min duration).orElse(Some(duration))
                runJobs(state, currentJobs.drop(1), iterationNumber + 1, sleepFor)
            }
          } else {
            runJobs(state, currentJobs.drop(1), iterationNumber + 1, sleep)
          }

        case None =>
          sleep match {
            case Some(duration) if duration.fromNow.hasTimeLeft() =>
              Compaction.sleep(state, duration)

            case Some(_) | None =>
              runJobs(state, state.levels.sorted, 1, None)
          }
      }

  private[compaction] def runJob(level: LevelRef): LevelCompactionState =
    level match {
      case zero: LevelZero =>
        pushForward(zero)

      case level: NextLevel =>
        pushForward(level)

      case TrashLevel =>
        LevelCompactionState.Idle
    }

  private[compaction] def pushForward(level: NextLevel): LevelCompactionState = {
    val throttle = level.throttle(level.meter)
    if (throttle.pushDelay.fromNow.isOverdue())
      pushForward(level, throttle.segmentsToPush) match {
        case IO.Success(_) =>
          LevelCompactionState.Idle

        case later @ IO.Later(_, _) =>
          val state = LevelCompactionState.AwaitingPull(_ready = false, timeout = awaitPullTimeout)
          later mapAsync {
            _ =>
              state.ready = true
          }
          state

        case IO.Failure(_) =>
          LevelCompactionState.Failed
      }
    else
      LevelCompactionState.Sleeping(throttle.pushDelay)
  }

  private[compaction] def pushForward(zero: LevelZero): LevelCompactionState =
    zero.nextLevel map {
      nextLevel =>
        pushForward(
          zero,
          nextLevel
        )
    } getOrElse LevelCompactionState.Idle

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
        LevelCompactionState.Idle
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
        LevelCompactionState.Idle

      case IO.Failure(exception) =>
        exception match {
          //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
          case IO.Error.OverlappingPushSegment =>
            logger.debug(s"{}: Failed to push. Waiting for pull. Cause - {}", zero.path, IO.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
          case _ =>
            logger.debug(s"{}: Failed to push. Waiting for pull", zero.path, exception)
        }
        LevelCompactionState.Failed

      case later @ IO.Later(_, _) =>
        val state = LevelCompactionState.AwaitingPull(_ready = false, timeout = awaitPullTimeout)
        later mapAsync {
          _ =>
            state.ready = true
        }
        state
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
  private[compaction] def copyForwardOnAllLevels(level: NextLevel): Int =
    level.reverseNextLevels.foldLeft[Int](0) {
      (totalCopied, level) =>
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
                totalCopied + copied

              case IO.Failure(error) =>
                logger.error(s"Failed copy Segments forward for level: ${level.rootPath}", error.exception)
                totalCopied

              case IO.Later(_, _) =>
                //this should never really occur when no other concurrent compactions are occurring.
                logger.warn(s"Received later compaction for level: ${level.rootPath}.")
                totalCopied
            }
        } getOrElse totalCopied
    }

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

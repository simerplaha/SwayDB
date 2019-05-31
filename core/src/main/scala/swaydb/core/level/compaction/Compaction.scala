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
import scala.concurrent.duration._


/**
  * Implements functions to execution Compaction on [[CompactionState]].
  * Once executed Compaction will run infinitely until shutdown.
  *
  * The speed of compaction depends on [[swaydb.core.level.Level.throttle]] which
  * is used to determine how overflow is a Level and how often should compaction run.
  */
private[level] object Compaction extends CompactionStrategy[CompactionState] with LazyLogging {

  val awaitPullTimeout = 30.seconds.fromNow

  def copyAndStart(state: CompactionState)(implicit ec: ExecutionContext): IO[Unit] =
    IO(wakeUp(state = state, forwardCopyOnAllLevels = true))

  def start(state: CompactionState)(implicit ec: ExecutionContext): IO[Unit] =
    IO(wakeUp(state = state, forwardCopyOnAllLevels = false))

  //wakeUp call received from Levels.
  def wakeUpFromZero(state: CompactionState)(implicit ec: ExecutionContext): Unit = {
    logger.trace(s"WakeUp from Zero. Running: ${state.running}")
    state.zeroWakeUpCalls.incrementAndGet()
    wakeUp(state = state, forwardCopyOnAllLevels = false)
  }

  def terminate(state: CompactionState): Unit =
    state.terminate = true

  /**
    * Mutates current state. Do not create copies for memory sake.
    */
  private[compaction] def resetState(state: CompactionState): CompactionState = {
    state.sleepTask foreach {
      task =>
        task.cancel()
        state.sleepTask = None
    }
    state.compactionStates.clear()
    state
  }

  def rePrioritiseLevels(levels: List[LevelRef])(implicit ordering: Ordering[LevelRef]): Slice[LevelRef] =
    Slice(levels.sorted.toArray)

  private[compaction] def wakeUp(state: CompactionState,
                                 forwardCopyOnAllLevels: Boolean)(implicit ec: ExecutionContext): Unit =
    if (state.terminate)
      logger.debug("Terminated! Ignoring wakeUp call.")
    else if (state.running.compareAndSet(false, true)) {
      //begin compaction
      val newState = resetState(state)
      if (forwardCopyOnAllLevels) {
        val totalCopies = copyForwardForEach(newState.levelsReversed)
        logger.debug(s"Compaction copied $totalCopies. Starting compaction!")
      }
      //run compaction jobs
      val sleepFor =
        runJobs(
          state = newState,
          currentJobs = rePrioritiseLevels(newState.levels)(state.ordering),
          wakeUpCallNumber = newState.zeroWakeUpCalls.get(),
        )(state.ordering)

      sleep(
        state = newState,
        duration = sleepFor
      )(state.ordering, ec)

      //on complete ask other lower compactions to wakeup if not already running.
      state.lowerCompactions foreach {
        state =>
          Compaction.wakeUp(state = state, forwardCopyOnAllLevels = forwardCopyOnAllLevels)
      }
    }

  private[compaction] def sleep(state: CompactionState,
                                duration: Deadline)(implicit ordering: Ordering[LevelRef],
                                                    ec: ExecutionContext): Unit =
    if (!state.terminate && state.running.compareAndSet(true, false)) {
      val task = Delay.task(duration.timeLeft)(wakeUp(state, forwardCopyOnAllLevels = true))
      state.sleepTask = Some(task)
    }

  def shouldRun(state: LevelCompactionState): Boolean =
    state match {
      case LevelCompactionState.AwaitingPull(ready, timeout) =>
        ready || timeout.isOverdue()

      case LevelCompactionState.Sleep(ready) =>
        ready.isOverdue()
    }

  /**
    * Should only be executed by [[wakeUpFromZero]] or by itself.
    */

  private[compaction] def runJobs(state: CompactionState,
                                  wakeUpCallNumber: Int,
                                  currentJobs: Slice[LevelRef])(implicit ordering: Ordering[LevelRef]): Deadline = {
    @tailrec
    def doRun(wakeUpCallNumber: Int,
              currentJobs: Slice[LevelRef],
              sleepDeadline: Deadline)(implicit ordering: Ordering[LevelRef]): Deadline =
      if (state.terminate)
        sleepDeadline
      else
        currentJobs.headOption match {
          //project next job
          case Some(level) =>
            //before each job check if there was a wakeUp call from Levels and re-prioritise compactions.
            val currentWakeUpCall = state.zeroWakeUpCalls.get()
            //if there was a wakeup call then check if the current job is the last job only then re-prioritise or else keep processing.
            //if LevelZero is higher priority then a dedicated compaction thread should be used when configuration compaction in LevelZero.
            //check for currentJobs <=1 is necessary to ensure only Level0 compaction does not always gets run for cases where concurrent compaction is 1.
            if (currentWakeUpCall != wakeUpCallNumber && currentJobs.size <= 1) {
              doRun(currentWakeUpCall, rePrioritiseLevels(state.levels), sleepDeadline)
            } else if (state.compactionStates.get(level).forall(shouldRun)) {
              val newLevelState = runJob(level)
              state.compactionStates.put(level, newLevelState)
              if (newLevelState.sleepDeadline < sleepDeadline)
                newLevelState.sleepDeadline
              else
                sleepDeadline
            } else {
              doRun(wakeUpCallNumber, currentJobs.dropHead(), sleepDeadline)
            }

          case None =>
            //all jobs complete.
            sleepDeadline
        }

    doRun(
      wakeUpCallNumber = wakeUpCallNumber,
      currentJobs = currentJobs,
      sleepDeadline = LevelCompactionState.longSleepDeadline
    )
  }

  private[compaction] def runJob(level: LevelRef): LevelCompactionState =
    level match {
      case zero: LevelZero =>
        pushForward(zero)

      case level: NextLevel =>
        pushForward(level)

      case TrashLevel =>
        logger.error(s"Received job for ${TrashLevel.getClass.getSimpleName}.")
        LevelCompactionState.longSleep
    }

  @tailrec
  private[compaction] def pushForward(level: NextLevel): LevelCompactionState = {
    val throttle = level.throttle(level.meter)
    if (throttle.pushDelay.fromNow.isOverdue())
      pushForward(level, throttle.segmentsToPush max 1) match {
        case IO.Success(_) =>
          pushForward(level)

        case later @ IO.Later(_, _) =>
          val state = LevelCompactionState.AwaitingPull(_ready = false, timeout = awaitPullTimeout)
          later mapAsync {
            _ =>
              state.ready = true
          }
          state

        case IO.Failure(_) =>
          LevelCompactionState.Sleep(3.second.fromNow)
      }
    else
      LevelCompactionState.Sleep(throttle.pushDelay.fromNow)
  }

  private[compaction] def pushForward(zero: LevelZero): LevelCompactionState =
    zero.nextLevel map {
      nextLevel =>
        pushForward(
          zero,
          nextLevel
        )
    } getOrElse LevelCompactionState.longSleep

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
        LevelCompactionState.longSleep
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
        LevelCompactionState.longSleep

      case IO.Failure(exception) =>
        exception match {
          //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
          case IO.Error.OverlappingPushSegment =>
            logger.debug(s"{}: Failed to push", zero.path, IO.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
          case _ =>
            logger.error(s"{}: Failed to push", zero.path, exception)
        }
        LevelCompactionState.longSleep

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
  private[compaction] def copyForwardForEach(levels: Seq[LevelRef]): Int =
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

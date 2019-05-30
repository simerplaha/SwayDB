package swaydb.core.level.compaction

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.core.util.Delay
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * Implements functions to execution Compaction on [[Compaction.State]].
  * Once executed Compaction will run infinitely until shutdown.
  *
  * The speed of compaction depends on [[swaydb.core.level.Level.throttle]] which
  * is used to determine how overflow is a Level and how often should compaction run.
  */
object Compaction extends LazyLogging {

  object State {
    def apply(zero: LevelZero,
              running: AtomicBoolean,
              compactionState: ConcurrentHashMap[LevelRef, CompactionState]): State =
      new State(
        zero = zero,
        levels = LevelRef.getLevels(zero),
        running = running,
        hasMaps = new AtomicBoolean(true),
        compactionState = compactionState
      )
  }

  /**
    * The state of compaction.
    */
  case class State(zero: LevelZero,
                   levels: List[LevelRef],
                   running: AtomicBoolean,
                   hasMaps: AtomicBoolean,
                   compactionState: ConcurrentHashMap[LevelRef, CompactionState])

  def bootUp(state: State)(implicit ordering: Ordering[LevelRef],
                           ec: ExecutionContext): IO[Unit] =
    if (state.running.compareAndSet(false, true))
      state.zero.nextLevel map {
        case level: NextLevel =>
          //On bootUp try copying all non overlapping Segments forward and start job.
          //TODO - there should be an config option to run this sync and async on current thread.
          copyForwardAll(level) onCompleteSideEffect {
            case Success(copied) =>
              logger.debug(s"Forward copied $copied Segments. Starting compaction!")
              runJobs(state, state.levels.sorted, 1)

            case Failure(error) =>
              logger.error("Failed to start compaction with copy all", error.exception)
              runJobs(state, state.levels.sorted, 1)
          }

          IO.unit

        case TrashLevel =>
          IO.unit
      } getOrElse IO.unit
    else
      IO.unit

  def wakeUp(state: State)(implicit ordering: Ordering[LevelRef],
                           ec: ExecutionContext): Unit = {
    state.hasMaps.compareAndSet(false, true)
    if (state.running.compareAndSet(false, true))
      runJobs(state, state.levels.sorted, 1)
  }

  private[compaction] def sleep(state: State,
                                duration: FiniteDuration)(implicit ordering: Ordering[LevelRef],
                                                          ec: ExecutionContext) =
    Delay.future(duration)(runJobs(state, state.levels.sorted, 1))

  def shouldRun(state: CompactionState): Boolean =
    state match {
      case CompactionState.AwaitingPull(ready) =>
        ready

      case CompactionState.Sleep(ready) =>
        ready.fromNow.isOverdue()

      case CompactionState.Idle | CompactionState.Failed =>
        true
    }

  @tailrec
  private[compaction] def runJobs(state: State,
                                  jobs: List[LevelRef],
                                  iterationNumber: Int)(implicit ordering: Ordering[LevelRef],
                                                        ec: ExecutionContext): Unit =
    jobs.headOption match {
      case Some(level) =>
        //if there is a wake up call from LevelZero and two compaction cycles have already run
        //then re-prioritise Levels and run compaction.
        if (iterationNumber >= 2 && state.hasMaps.compareAndSet(true, false)) {
          runJobs(state, state.levels.sorted, 1)
        } else if (shouldRun(state.compactionState.getOrDefault(level, CompactionState.Idle))) {
          val newState = runJob(level)
          newState match {
            case CompactionState.AwaitingPull(_) | CompactionState.Idle | CompactionState.Failed =>
              state.compactionState.put(level, newState)
              runJobs(state, jobs.drop(1), iterationNumber + 1)

            case CompactionState.Sleep(duration) =>
              sleep(state, duration)
          }
        } else {
          runJobs(state, jobs.drop(1), iterationNumber + 1)
        }

      case None =>
        //jobs complete! re-prioritise and re-run.
        runJobs(state, state.levels.sorted, 1)
    }

  private[compaction] def runJob(level: LevelRef)(implicit ordering: Ordering[LevelRef]): CompactionState =
    level match {
      case zero: LevelZero =>
        pushForward(zero)

      case level: NextLevel =>
        pushForward(level)

      case TrashLevel =>
        CompactionState.Idle
    }

  private[compaction] def pushForward(level: NextLevel): CompactionState = {
    val throttle = level.throttle(level.meter)
    if (throttle.pushDelay.fromNow.isOverdue())
      pushForward(level, throttle.segmentsToPush) match {
        case IO.Success(_) =>
          CompactionState.Idle

        case later @ IO.Later(_, _) =>
          val state = CompactionState.AwaitingPull(ready = false)
          later mapAsync {
            _ =>
              state.ready = true
          }
          state

        case IO.Failure(_) =>
          CompactionState.Failed
      }
    else
      CompactionState.Sleep(throttle.pushDelay)
  }

  private[compaction] def pushForward(zero: LevelZero): CompactionState =
    zero.nextLevel map {
      nextLevel =>
        pushForward(
          zero,
          nextLevel
        )
    } getOrElse CompactionState.Idle

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel): CompactionState =
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
        CompactionState.Idle
    }

  private[compaction] def pushForward(zero: LevelZero,
                                      nextLevel: NextLevel,
                                      map: swaydb.core.map.Map[Slice[Byte], Memory.SegmentResponse]): CompactionState =
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
        CompactionState.Idle

      case IO.Failure(exception) =>
        exception match {
          //do not log the stack if the IO.Failure to merge was ContainsOverlappingBusySegments.
          case IO.Error.OverlappingPushSegment =>
            logger.debug(s"{}: Failed to push. Waiting for pull. Cause - {}", zero.path, IO.Error.OverlappingPushSegment.getClass.getSimpleName.dropRight(1))
          case _ =>
            logger.debug(s"{}: Failed to push. Waiting for pull", zero.path, exception)
        }
        CompactionState.Failed

      case later @ IO.Later(_, _) =>
        val state = CompactionState.AwaitingPull(ready = false)
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
    } getOrElse IO.zero

  private[compaction] def copyForwardAll(level: NextLevel): IO[Int] =
    level.reverseNextLevels.foldLeftIO[Int](r = 0, failFast = false) {
      (totalCopied, nextLevel) =>
        val (copyable, _) = nextLevel.partitionUnreservedCopyable(level.segmentsInLevel())
        putForward(
          segments = copyable,
          thisLevel = level,
          nextLevel = nextLevel
        ) match {
          case IO.Success(copied) =>
            IO.Success(totalCopied + copied)

          case IO.Later(_, _) | IO.Failure(_) =>
            IO.Success(totalCopied)
        }
    }

  private[compaction] def putForward(segments: Iterable[Segment],
                                     thisLevel: NextLevel,
                                     nextLevel: NextLevel): IO.Async[Int] =
    if (segments.isEmpty)
      IO.zero
    else
      nextLevel.put(segments) mapAsync (_ => segments.size)
}
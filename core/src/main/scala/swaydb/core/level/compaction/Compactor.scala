package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.LevelRef
import swaydb.core.util.FiniteDurationUtil
import swaydb.data.IO
import swaydb.data.IO._
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

/**
  * Compactor = Compaction-Actor.
  */
object Compactor {

  def apply(levels: List[LevelRef],
            executionContexts: List[CompactionExecutionContext])(implicit ordering: CompactionOrdering,
                                                                 compactionStrategy: CompactionStrategy[CompactorState]): IO[WiredActor[CompactionStrategy[CompactorState], CompactorState]] =
  //split levels into groups such that each group of Levels have dedicated ExecutionContext.
  //if dedicatedLevelZeroCompaction is set to true set the first ExecutionContext for LevelZero.
    levels
      .zip(executionContexts)
      .foldLeftIO(ListBuffer.empty[(ListBuffer[LevelRef], ExecutionContext)]) {
        case (jobs, (level, executionContext)) =>
          executionContext match {
            case CompactionExecutionContext.Create(executionContext) =>
              jobs += ((ListBuffer(level), executionContext))
              IO.Success(jobs)

            case CompactionExecutionContext.Shared =>
              jobs.lastOption match {
                case Some((lastGroup, _)) =>
                  lastGroup += level
                  IO.Success(jobs)

                case None =>
                  //this will never occur because during configuration Level0 is only allowed to have Create
                  //so Shared can never happen with Create.
                  IO.Failure(new IllegalStateException("Shared ExecutionContext submitted without Create."))
              }
          }
      }
      .map {
        jobs =>
          jobs
            .reverse
            .foldRight(Option.empty[WiredActor[CompactionStrategy[CompactorState], CompactorState]]) {
              case ((jobs, executionContext), child) =>
                val statesMap = mutable.Map.empty[LevelRef, LevelCompactionState]
                val levelOrdering = ordering.ordering(level => statesMap.getOrElse(level, LevelCompactionState.longSleep(level.stateID)))
                val compaction =
                  CompactorState(
                    levels = Slice(jobs.toArray),
                    compactionStates = statesMap,
                    executionContext = executionContext,
                    child = child,
                    ordering = levelOrdering
                  )

                val actor =
                  WiredActor[CompactionStrategy[CompactorState], CompactorState](
                    impl = compactionStrategy,
                    state = compaction
                  )(executionContext)

                Some(actor)
            } head
      }
}

class Compactor extends CompactionStrategy[CompactorState] {

  private def scheduleNextWakeUp(state: CompactorState,
                                 self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    state
      .compactionStates
      .values
      .foldLeft(Option(LevelCompactionState.longSleepDeadline)) {
        case (nearestDeadline, waiting @ LevelCompactionState.AwaitingPull(ioAync, timeout, _)) =>
          //do not create another hook if a future was already initialised to invoke wakeUp.
          if (!waiting.listenerInitialised) {
            ioAync.safeGetFuture(self.ec).foreach {
              _ =>
                waiting.isReady = true
                self send {
                  (impl, state, self) =>
                    impl.wakeUp(
                      state = state,
                      forwardCopyOnAllLevels = false,
                      self = self
                    )
                }
            }(self.ec)
            waiting.listenerInitialised = true
          }

          FiniteDurationUtil.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(timeout)
          )

        case (nearestDeadline, LevelCompactionState.Sleep(sleepDeadline, _)) =>
          FiniteDurationUtil.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(sleepDeadline)
          )
      }
      .foreach {
        newWakeUpDeadline =>
          //if the wakeUp deadlines are the same do not trigger another wakeUp.
          if (state.sleepTask.forall(_._2.compareTo(newWakeUpDeadline) != 0)) {
            val newTask =
              self.scheduleSend(newWakeUpDeadline.timeLeft) {
                (impl, state) =>
                  impl.wakeUp(
                    state = state,
                    forwardCopyOnAllLevels = false,
                    self = self
                  )
              }
            state.sleepTask foreach (_._1.cancel())
            state.sleepTask = Some(newTask, newWakeUpDeadline)
          }
      }

  private def wakeUpChildren(state: CompactorState): Unit =
    state
      .child
      .foreach {
        child =>
          child send {
            (impl, state, self) =>
              impl.wakeUp(
                state = state,
                forwardCopyOnAllLevels = false,
                self = self
              )
          }
      }

  def postCompaction[T](state: CompactorState,
                        self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit = {
    //schedule the next compaction for current Compaction group levels
    scheduleNextWakeUp(
      state = state,
      self = self
    )

    //wake up child compaction.
    wakeUpChildren(
      state = state
    )
  }

  override def wakeUp(state: CompactorState,
                      forwardCopyOnAllLevels: Boolean,
                      self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    try
      Compaction.run(
        state = state,
        forwardCopyOnAllLevels = false
      )
    finally
      postCompaction(
        state = state,
        self = self
      )
}

package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.core.segment.Segment
import swaydb.core.util.CollectionUtil

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Deadline

/**
  * Compactor - Compaction-Actor.
  */
object Compactor {

  def apply[S](compactionStrategy: CompactionStrategy[S],
               state: S)(implicit ec: ExecutionContext): WiredActor[CompactionStrategy[S], S] =
    WiredActor[CompactionStrategy[S], S](
      impl = compactionStrategy,
      state = state
    )

  def apply(zero: LevelZero,
            levels: List[NextLevel],
            executionContexts: Seq[ExecutionContext],
            concurrentCompactions: Int,
            dedicatedLevelZeroCompaction: Boolean)(implicit ordering: CompactionOrdering,
                                                   compactionStrategy: CompactionStrategy[CompactorState]): WiredActor[CompactionStrategy[CompactorState], CompactorState] =
    CollectionUtil
      .groupedNoSingles(
        concurrentCompactions = concurrentCompactions,
        items = levels,
        splitAt = if (dedicatedLevelZeroCompaction) 1 else 0
      )
      .reverse
      .zip(executionContexts)
      .foldRight(Option.empty[WiredActor[CompactionStrategy[CompactorState], CompactorState]]) {
        case ((jobs, executionContext), child) =>
          val statesMap = mutable.Map.empty[LevelRef, LevelCompactionState]
          val levelOrdering = ordering.ordering(level => statesMap.getOrElse(level, LevelCompactionState.longSleep(level.stateID)))
          val compaction =
            CompactorState(
              levels = jobs,
              compactionStates = statesMap,
              child = child,
              ordering = levelOrdering
            )
          Some(Compactor(compactionStrategy, compaction)(executionContext))
      } head
}

class Compactor extends CompactionStrategy[CompactorState] {

  private def scheduleNextCompaction(state: CompactorState,
                                     self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    state
      .compactionStates
      .values
      .foldLeft(Option.empty[Deadline]) {
        case (nearestDeadline, state) =>
          state match {
            case waiting @ LevelCompactionState.AwaitingPull(ioAync, timeout, _) =>
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

              Segment.getNearestDeadline(nearestDeadline, Some(timeout))

            case LevelCompactionState.Sleep(sleepDeadline, _) =>
              Segment.getNearestDeadline(nearestDeadline, Some(sleepDeadline))
          }
      }
      .foreach {
        deadline =>
          val newTask =
            self.scheduleSend(deadline.timeLeft) {
              (impl, state) =>
                impl.wakeUp(
                  state = state,
                  forwardCopyOnAllLevels = false,
                  self = self
                )
            }
          state.sleepTask = Some(newTask)
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
                        self: WiredActor[CompactionStrategy[CompactorState], CompactorState])(run: => T): T =
    try {
      state.sleepTask foreach (_.cancel())
      state.sleepTask = None
      run
    } finally {
      //wake up child compaction.
      wakeUpChildren(
        state = state
      )

      //schedule the next compaction for current Compaction group levels
      scheduleNextCompaction(
        state = state,
        self = self
      )
    }

  override def wakeUp(state: CompactorState, forwardCopyOnAllLevels: Boolean, self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    postCompaction(state, self) {
      Compaction.run(
        state = state,
        forwardCopyOnAllLevels = false
      )
    }
}

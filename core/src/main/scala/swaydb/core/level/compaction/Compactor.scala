package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.core.segment.Segment
import swaydb.core.util.CollectionUtil
import swaydb.data.slice.Slice

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
                                                   compactionStrategy: CompactionStrategy[CompactionGroupState]): WiredActor[CompactionStrategy[CompactionGroupState], CompactionGroupState] =
    CollectionUtil
      .groupedMergeSingles(
        groupSize = concurrentCompactions,
        items = levels,
        splitAt = if (dedicatedLevelZeroCompaction) 1 else 0
      )
      .reverse
      .zip(executionContexts)
      .foldRight(Option.empty[WiredActor[CompactionStrategy[CompactionGroupState], CompactionGroupState]]) {
        case ((jobs, executionContext), child) =>
          val statesMap = mutable.Map.empty[LevelRef, LevelCompactionState]
          val levelOrdering = ordering.ordering(level => statesMap.getOrElse(level, LevelCompactionState.longSleep(level.stateID)))
          val compaction =
            CompactionGroupState(
              levels = Slice(jobs.toArray),
              compactionStates = statesMap,
              child = child,
              ordering = levelOrdering
            )
          Some(Compactor(compactionStrategy, compaction)(executionContext))
      } head
}

class Compactor extends CompactionStrategy[CompactionGroupState] {

  private def scheduleNextCompaction(state: CompactionGroupState,
                                     self: WiredActor[CompactionStrategy[CompactionGroupState], CompactionGroupState]): Unit =
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

  private def wakeUpChildren(state: CompactionGroupState): Unit =
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

  def postCompaction[T](state: CompactionGroupState,
                        self: WiredActor[CompactionStrategy[CompactionGroupState], CompactionGroupState])(run: => T): T =
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

  override def wakeUp(state: CompactionGroupState, forwardCopyOnAllLevels: Boolean, self: WiredActor[CompactionStrategy[CompactionGroupState], CompactionGroupState]): Unit =
    postCompaction(state, self) {
      Compaction.run(
        state = state,
        forwardCopyOnAllLevels = false
      )
    }
}

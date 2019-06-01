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
import swaydb.core.util.FiniteDurationUtil._

/**
  * Compactor = Compaction-Actor.
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
            dedicatedLevelZeroCompaction: Boolean)(implicit ordering: CompactionOrdering,
                                                   compactionStrategy: CompactionStrategy[CompactorState]): WiredActor[CompactionStrategy[CompactorState], CompactorState] = {
    //split levels into groups such that each group of Levels have dedicated ExecutionContext.
    //if dedicatedLevelZeroCompaction is set to true set the first ExecutionContext for LevelZero.

    val groupSizeDouble =
      if (dedicatedLevelZeroCompaction)
        (levels.size - 1).toDouble / (executionContexts.size - 1).toDouble
      else
        levels.size.toDouble / executionContexts.size.toDouble

    CollectionUtil
      .groupedMergeSingles(
        groupSize = Math.ceil(groupSizeDouble).toInt,
        items = zero +: levels,
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
              levels = Slice(jobs.toArray),
              compactionStates = statesMap,
              child = child,
              ordering = levelOrdering
            )
          Some(Compactor(compactionStrategy, compaction)(executionContext))
      } head
  }
}

class Compactor extends CompactionStrategy[CompactorState] {

  private def scheduleNextWakeUp(state: CompactorState,
                                 self: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    state
      .compactionStates
      .values
      .foldLeft(Option.empty[Deadline]) {
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

          Segment.getNearestDeadline(
            deadline = nearestDeadline,
            next = Some(timeout)
          )

        case (nearestDeadline, LevelCompactionState.Sleep(sleepDeadline, _)) =>
          Segment.getNearestDeadline(
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

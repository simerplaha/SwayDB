package swaydb.core.level.compaction

import java.util.TimerTask
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{LevelRef, NextLevel}
import swaydb.core.util.CollectionUtil

import scala.collection.mutable
import scala.concurrent.ExecutionContext

private[level] object CompactionState {

  private def createCompactions(concurrentCompactions: Int,
                                levels: List[LevelRef],
                                executionContexts: Seq[ExecutionContext])(implicit ordering: CompactionOrdering): List[CompactionState] =
    CollectionUtil
      .groupedNoSingles(concurrentCompactions, levels)
      .reverse
      .zip(executionContexts)
      .foldRight(List.empty[CompactionState]) {
        case ((jobs, executionContext), lowerCompactions) =>
          val statesMap = mutable.Map.empty[LevelRef, LevelCompactionState]
          val levelOrdering = ordering.ordering(level => statesMap.getOrElse(level, LevelCompactionState.longSleep))
          val compaction =
            CompactionState(
              levels = jobs,
              running = new AtomicBoolean(false),
              zeroWakeUpCalls = new AtomicInteger(0),
              compactionStates = statesMap,
              ordering = levelOrdering,
              lowerCompactions = lowerCompactions,
              executionContext = executionContext
            )
          compaction +: lowerCompactions
      }

  def apply(zero: LevelZero,
            levels: List[NextLevel],
            executionContexts: Seq[ExecutionContext],
            concurrentCompactions: Int,
            dedicatedLevelZeroCompaction: Boolean)(implicit ordering: CompactionOrdering): List[CompactionState] =
    if (dedicatedLevelZeroCompaction)
      createCompactions(1, List(zero), executionContexts) ++ createCompactions((concurrentCompactions - 1) max 1, levels, executionContexts)
    else
      createCompactions(concurrentCompactions max 1, zero +: levels, executionContexts)
}

/**
  * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
  */
private[level] case class CompactionState(levels: List[LevelRef],
                                          private[compaction] val running: AtomicBoolean,
                                          private[compaction] val zeroWakeUpCalls: AtomicInteger,
                                          private[level] val compactionStates: mutable.Map[LevelRef, LevelCompactionState],
                                          private[compaction] val lowerCompactions: List[CompactionState],
                                          ordering: Ordering[LevelRef],
                                          executionContext: ExecutionContext) {
  @volatile private[compaction] var terminate: Boolean = false
  private[compaction] var sleepTask: Option[TimerTask] = None

  val levelsReversed = levels.reverse
}

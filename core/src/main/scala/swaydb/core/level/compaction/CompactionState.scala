package swaydb.core.level.compaction

import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import swaydb.core.level.LevelRef
import swaydb.core.util.CollectionUtil

private[level] object CompactionState {
  def apply(levels: List[LevelRef],
            concurrentCompactions: Int)(implicit ordering: CompactionOrdering): List[CompactionState] =
    CollectionUtil.groupedNoSingles(
      concurrentCompactions = concurrentCompactions,
      items = levels
    ) map {
      jobs =>
        val statesMap = new ConcurrentHashMap[LevelRef, LevelCompactionState]()
        val levelOrdering = ordering.ordering(statesMap.getOrDefault(_, LevelCompactionState.Idle))
        new CompactionState(
          levels = jobs,
          running = new AtomicBoolean(false),
          zeroReady = new AtomicBoolean(true),
          compactionStates = statesMap,
          ordering = levelOrdering
        )
    }
}

/**
  * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
  */
private[level] case class CompactionState(levels: List[LevelRef],
                                          private[compaction] val running: AtomicBoolean,
                                          private[compaction] val zeroReady: AtomicBoolean,
                                          private[level] val compactionStates: ConcurrentHashMap[LevelRef, LevelCompactionState],
                                          ordering: Ordering[LevelRef]) {
  @volatile private[compaction] var sleepTask: Option[TimerTask] = None
  @volatile private[compaction] var terminate: Boolean = false
  val levelsReversed = levels.reverse
}

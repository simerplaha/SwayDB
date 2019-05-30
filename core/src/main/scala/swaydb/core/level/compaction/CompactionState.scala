package swaydb.core.level.compaction

import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import swaydb.core.level.LevelRef
import swaydb.core.level.zero.LevelZero

private[level] object CompactionState {
  def apply(zero: LevelZero,
            concurrentCompactions: Int): CompactionState =
    new CompactionState(
      zero = zero,
      concurrentCompactions = concurrentCompactions,
      levels = LevelRef.getLevels(zero),
      running = new AtomicBoolean(false),
      zeroReady = new AtomicBoolean(true),
      compactionStates = new ConcurrentHashMap[LevelRef, LevelCompactionState]()
    )
}

/**
  * The state of compaction.
  */
private[level] case class CompactionState(zero: LevelZero,
                                          concurrentCompactions: Int,
                                          private[compaction] val levels: List[LevelRef],
                                          private[compaction] val running: AtomicBoolean,
                                          private[compaction] val zeroReady: AtomicBoolean,
                                          private[compaction] val compactionStates: ConcurrentHashMap[LevelRef, LevelCompactionState]) {
  @volatile private[compaction] var sleepTask: Option[TimerTask] = None
}

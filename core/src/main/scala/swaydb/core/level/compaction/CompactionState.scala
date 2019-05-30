package swaydb.core.level.compaction

import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import swaydb.core.level.LevelRef
import swaydb.core.level.zero.LevelZero


object CompactionState {
  def apply(zero: LevelZero,
            running: AtomicBoolean,
            levelStates: ConcurrentHashMap[LevelRef, LevelCompactionState],
            concurrentCompactions: Int): CompactionState =
    new CompactionState(
      zero = zero,
      levels = LevelRef.getLevels(zero),
      running = running,
      zeroReady = new AtomicBoolean(true),
      concurrentCompactions = concurrentCompactions,
      compactionStates = levelStates
    )
}

/**
  * The state of compaction.
  */
case class CompactionState(zero: LevelZero,
                           levels: List[LevelRef],
                           running: AtomicBoolean,
                           zeroReady: AtomicBoolean,
                           concurrentCompactions: Int,
                           compactionStates: ConcurrentHashMap[LevelRef, LevelCompactionState]) {
  @volatile var sleepTask: Option[TimerTask] = None
}

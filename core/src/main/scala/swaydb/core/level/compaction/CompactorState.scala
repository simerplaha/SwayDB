package swaydb.core.level.compaction

import java.util.TimerTask

import swaydb.core.actor.WiredActor
import swaydb.core.level.LevelRef

import scala.collection.mutable

/**
  * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
  */
private[core] case class CompactorState(levels: List[LevelRef],
                                        child: Option[WiredActor[CompactionStrategy[CompactorState], CompactorState]],
                                        ordering: Ordering[LevelRef],
                                        private[level] val compactionStates: mutable.Map[LevelRef, LevelCompactionState]) {
  @volatile private[compaction] var terminate: Boolean = false
  private[compaction] var sleepTask: Option[TimerTask] = None
  val hasLevelZero: Boolean = levels.exists(_.isZero)
  val levelsReversed = levels.reverse

  def isLevelStateChanged() =
    levels exists {
      level =>
        compactionStates.get(level) forall (_.previousStateID != level.stateID)
    }

  def terminateCompaction() =
    terminate = true
}

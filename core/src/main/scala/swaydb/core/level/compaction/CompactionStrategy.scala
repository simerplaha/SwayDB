package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor
import swaydb.core.level.zero.LevelZero
import swaydb.data.IO
import swaydb.data.compaction.CompactionExecutionContext

private[level] trait CompactionStrategy[S] {

  def createAndStart(zero: LevelZero,
                     executionContexts: List[CompactionExecutionContext],
                     copyForwardAllOnStart: Boolean)(implicit compactionStrategy: CompactionStrategy[CompactorState],
                                                     compactionOrdering: CompactionOrdering): IO[WiredActor[CompactionStrategy[CompactorState], CompactorState]]

  def wakeUp(state: S,
             forwardCopyOnAllLevels: Boolean,
             self: WiredActor[CompactionStrategy[S], S]): Unit

  def terminate(compactor: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit
}

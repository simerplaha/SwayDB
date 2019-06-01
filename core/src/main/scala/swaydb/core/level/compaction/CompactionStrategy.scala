package swaydb.core.level.compaction

import swaydb.core.actor.WiredActor

private[level] trait CompactionStrategy[S] {

  def wakeUp(state: S, forwardCopyOnAllLevels: Boolean, self: WiredActor[CompactionStrategy[S], S]): Unit
}

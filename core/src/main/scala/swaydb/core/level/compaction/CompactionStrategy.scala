package swaydb.core.level.compaction

import swaydb.data.IO

private[level] trait CompactionStrategy[S] {

  def start(state: S): IO[Unit]

  def copyAndStart(state: S): IO[Unit]

  def wakeUpFromZero(state: S): Unit

  def terminate(state: CompactionState): Unit
}

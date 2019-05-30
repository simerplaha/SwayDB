package swaydb.core.level.compaction

import swaydb.core.level.LevelRef
import swaydb.data.IO

import scala.concurrent.ExecutionContext

private[level] trait CompactionStrategy[S] {

  def start(state: S)(implicit ordering: Ordering[LevelRef],
                      ec: ExecutionContext): IO[Unit]

  def copyAndStart(state: S)(implicit ordering: Ordering[LevelRef],
                             ec: ExecutionContext): IO[Unit]

  def zeroReady(state: S)(implicit ordering: Ordering[LevelRef],
                          ec: ExecutionContext): Unit
}

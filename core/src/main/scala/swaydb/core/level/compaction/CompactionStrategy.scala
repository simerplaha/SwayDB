package swaydb.core.level.compaction

import swaydb.data.IO

import scala.concurrent.ExecutionContext

private[level] trait CompactionStrategy[S] {

  def start(state: S)(implicit ec: ExecutionContext): IO[Unit]

  def copyAndStart(state: S)(implicit ec: ExecutionContext): IO[Unit]

  def wakeUpFromZero(state: S)(implicit ec: ExecutionContext): Unit

  def terminate(state: CompactionState): Unit
}

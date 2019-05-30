package swaydb.core.level.compaction

import scala.concurrent.duration.FiniteDuration

sealed trait CompactionState
object CompactionState {
  case class AwaitingPull(@volatile var ready: Boolean) extends CompactionState
  case object Idle extends CompactionState
  case class Sleep(duration: FiniteDuration) extends CompactionState
  case object Failed extends CompactionState
}

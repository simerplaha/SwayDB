package swaydb.core.level.compaction

import scala.concurrent.duration.FiniteDuration

sealed trait CompactionState
object CompactionState {
  case class AwaitingPull(private val _ready: Boolean) extends CompactionState {
    @volatile var ready: Boolean = _ready
  }
  case object Idle extends CompactionState
  case class Sleep(duration: FiniteDuration) extends CompactionState
  case object Failed extends CompactionState
}

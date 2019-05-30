package swaydb.core.level.compaction


import scala.concurrent.duration.{Deadline, FiniteDuration}

sealed trait LevelCompactionState
object LevelCompactionState {
  case class AwaitingPull(private val _ready: Boolean, timeout: Deadline) extends LevelCompactionState {
    @volatile var ready: Boolean = _ready
  }
  case object Idle extends LevelCompactionState
  case class Sleep(duration: FiniteDuration) extends LevelCompactionState
  case object Failed extends LevelCompactionState
}

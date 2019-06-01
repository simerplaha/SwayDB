package swaydb.core.level.compaction


import scala.concurrent.duration.{Deadline, _}

private[level] sealed trait LevelCompactionState {
  def sleepDeadline: Deadline
  def previousStateID: Long
}
private[level] object LevelCompactionState {
  val longSleepDuration = 1.hour
  def longSleepDeadline = longSleepDuration.fromNow

  case class AwaitingPull(private val _ready: Boolean, timeout: Deadline, previousStateID: Long) extends LevelCompactionState {
    @volatile var ready: Boolean = _ready
    override def sleepDeadline: Deadline = timeout
  }
  case class Sleep(sleepDeadline: Deadline, previousStateID: Long) extends LevelCompactionState

  def longSleep(stateID: Long) = Sleep(longSleepDuration.fromNow, stateID)
}

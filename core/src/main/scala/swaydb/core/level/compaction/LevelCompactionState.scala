package swaydb.core.level.compaction


import scala.concurrent.duration.{Deadline, _}

private[level] sealed trait LevelCompactionState {
  def sleepDeadline: Deadline
}
private[level] object LevelCompactionState {
  val longSleepDuration = 1.hour
  def longSleepDeadline = longSleepDuration.fromNow

  case class AwaitingPull(private val _ready: Boolean, timeout: Deadline) extends LevelCompactionState {
    @volatile var ready: Boolean = _ready
    override def sleepDeadline: Deadline = timeout
  }
  case class Sleep(sleepDeadline: Deadline) extends LevelCompactionState

  def longSleep = Sleep(longSleepDuration.fromNow)
  def awake = Sleep(Deadline.now)
}

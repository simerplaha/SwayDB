package swaydb.core.level.compaction


import swaydb.data.IO

import scala.concurrent.duration.{Deadline, _}

private[level] sealed trait LevelCompactionState {
  def previousStateID: Long
}
private[level] object LevelCompactionState {
  val longSleepDuration = 1.hour
  def longSleepDeadline = longSleepDuration.fromNow

  case class AwaitingPull(later: IO.Later[_],
                          timeout: Deadline,
                          previousStateID: Long) extends LevelCompactionState {
    @volatile var isReady: Boolean = false
  }
  case class Sleep(sleepDeadline: Deadline, previousStateID: Long) extends LevelCompactionState

  def longSleep(stateID: Long) = Sleep(longSleepDuration.fromNow, stateID)
}

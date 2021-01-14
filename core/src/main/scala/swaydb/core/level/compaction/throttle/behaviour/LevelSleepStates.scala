package swaydb.core.level.compaction.throttle.behaviour

import swaydb.core.level.Level
import swaydb.core.level.compaction.throttle.ThrottleLevelState
import swaydb.core.level.zero.LevelZero

object LevelSleepStates {

  def success(zero: LevelZero,
              stateId: Long): ThrottleLevelState.Sleeping =
    ThrottleLevelState.Sleeping(
      sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else zero.nextCompactionDelay.fromNow,
      stateId = stateId
    )

  def failure(zero: LevelZero,
              stateId: Long): ThrottleLevelState.Sleeping =
    ThrottleLevelState.Sleeping(
      sleepDeadline = if (zero.levelZeroMeter.mapsCount == 1) ThrottleLevelState.longSleep else (ThrottleLevelState.failureSleepDuration min zero.nextCompactionDelay).fromNow,
      stateId = stateId
    )

  def success(level: Level,
              stateId: Long): ThrottleLevelState.Sleeping =
    ThrottleLevelState.Sleeping(
      sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else level.nextCompactionDelay.fromNow,
      stateId = stateId
    )

  def failure(level: Level,
              stateId: Long): ThrottleLevelState.Sleeping =
    ThrottleLevelState.Sleeping(
      sleepDeadline = if (level.isEmpty) ThrottleLevelState.longSleep else (ThrottleLevelState.failureSleepDuration min level.nextCompactionDelay).fromNow,
      stateId = stateId
    )
}

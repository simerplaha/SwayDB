/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.compaction.throttle.behaviour

import swaydb.core.compaction.throttle.LevelState
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero

protected object LevelSleepStates {

  def success(zero: LevelZero,
              stateId: Long): LevelState.Sleeping =
    LevelState.Sleeping(
      sleepDeadline = if (zero.levelZeroMeter.logsCount == 1) LevelState.longSleep else zero.nextCompactionDelay.fromNow,
      stateId = stateId
    )

  def failure(stateId: Long): LevelState.Sleeping =
    LevelState.Sleeping(
      sleepDeadline = LevelState.failureSleepDuration.fromNow,
      stateId = stateId
    )

  def success(level: Level,
              stateId: Long): LevelState.Sleeping =
    LevelState.Sleeping(
      sleepDeadline = if (level.isEmpty) LevelState.longSleep else level.nextCompactionDelay.fromNow,
      stateId = stateId
    )
}

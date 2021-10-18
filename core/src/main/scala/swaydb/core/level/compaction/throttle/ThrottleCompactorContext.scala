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

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.LevelRef
import swaydb.data.compaction.CompactionConfig
import swaydb.data.slice.Slice

import java.util.TimerTask
import scala.concurrent.duration.Deadline

protected case class ThrottleCompactorContext(levels: Slice[LevelRef],
                                              compactionConfig: CompactionConfig,
                                              compactionStates: Map[LevelRef, LevelState.Sleeping],
                                              sleepTask: Option[(TimerTask, Deadline)] = None,
                                              @volatile private var _terminateASAP: Boolean = false) extends LazyLogging {

  final def name = {
    val info = levels.map(_.levelNumber).mkString(", ")

    if (levels.size == 1)
      s"Level($info)"
    else
      s"Levels($info)"
  }

  def setTerminateASAP() =
    _terminateASAP = true

  def terminateASAP(): Boolean =
    _terminateASAP
}

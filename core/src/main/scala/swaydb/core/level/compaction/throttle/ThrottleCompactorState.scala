/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorWire
import swaydb.core.level.LevelRef
import swaydb.core.level.compaction.Compactor
import swaydb.data.slice.Slice

import java.util.TimerTask
import scala.concurrent.duration.Deadline

private[core] sealed trait ThrottleCompactorState {
  def levels: Slice[LevelRef]

  def context: ThrottleCompactorState.Context

  def updateContext(context: ThrottleCompactorState.Context): ThrottleCompactorState

  final def name =
    context.name
}

private[core] object ThrottleCompactorState {

  case class Terminated(context: Context) extends ThrottleCompactorState {
    override def levels: Slice[LevelRef] =
      context.levels

    override def updateContext(context: Context): Terminated =
      copy(context = context)
  }

  case class Sleeping(context: Context) extends ThrottleCompactorState {
    override def levels: Slice[LevelRef] =
      context.levels

    override def updateContext(context: Context): Sleeping =
      copy(context = context)
  }

  /**
   * Compaction state for a group of Levels. The number of compaction depends on concurrentCompactions input.
   */
  case class Context(levels: Slice[LevelRef],
                     resetCompactionPriorityAtInterval: Int,
                     child: Option[ActorWire[Compactor, Unit]],
                     compactionStates: Map[LevelRef, ThrottleLevelState],
                     sleepTask: Option[(TimerTask, Deadline)] = None,
                     @volatile private var _terminateASAP: Boolean = false) extends LazyLogging {

    final def name = {
      val info = levels.map(_.levelNumber).mkString(", ")

      if (levels.size == 1)
        s"Level($info)"
      else
        s"Levels($info)"
    }

    def setTerminated() =
      _terminateASAP = true

    def terminateASAP(): Boolean =
      _terminateASAP
  }
}

/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level.actor

import java.util.TimerTask

import swaydb.core.actor.ActorRef
import swaydb.core.level.actor.LevelCommand.Pull
import swaydb.core.segment.Segment

private[core] sealed trait LevelState {

  val collapseSmallSegmentsTaskScheduled: Boolean

  val task: Option[TimerTask]

  def busySegments: List[Segment]

  def isSleeping: Boolean

  def isPushing: Boolean

  def isScheduled: Boolean

  val waitingPull: Option[ActorRef[Pull]]

  def setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled: Boolean): LevelState

  def setTask(task: TimerTask): LevelState

  def clearTask(): LevelState

}

private[core] object LevelState {
  case class Pushing(busySegments: List[Segment],
                     collapseSmallSegmentsTaskScheduled: Boolean,
                     task: Option[TimerTask],
                     waitingPull: Option[ActorRef[Pull]]) extends LevelState {
    override def isSleeping: Boolean = false

    override def isPushing: Boolean = true

    override def isScheduled: Boolean = false

    override def setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled: Boolean): LevelState =
      copy(collapseSmallSegmentsTaskScheduled = collapseSmallSegmentsTaskScheduled)

    override def setTask(task: TimerTask): LevelState =
      copy(task = Some(task))

    override def clearTask(): LevelState =
      copy(task = None)
  }

  case class PushScheduled(collapseSmallSegmentsTaskScheduled: Boolean,
                           task: Option[TimerTask]) extends LevelState {
    override def isSleeping: Boolean = false

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = true

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled: Boolean): LevelState =
      copy(collapseSmallSegmentsTaskScheduled = collapseSmallSegmentsTaskScheduled)

    override def setTask(expireKeyValueScheduled: TimerTask): LevelState =
      copy(task = Some(expireKeyValueScheduled))

    override def clearTask(): LevelState =
      copy(task = None)
  }

  case class Sleeping(collapseSmallSegmentsTaskScheduled: Boolean,
                      task: Option[TimerTask]) extends LevelState {

    override def isSleeping: Boolean = true

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = false

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled: Boolean): LevelState =
      copy(collapseSmallSegmentsTaskScheduled = collapseSmallSegmentsTaskScheduled)

    override def setTask(expireKeyValueScheduled: TimerTask): LevelState =
      copy(task = Some(expireKeyValueScheduled))

    override def clearTask(): LevelState =
      copy(task = None)
  }

  case class WaitingPull(collapseSmallSegmentsTaskScheduled: Boolean,
                         task: Option[TimerTask]) extends LevelState {

    override def isSleeping: Boolean = true

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = false

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def setCollapseSmallSegmentScheduled(collapseSmallSegmentsTaskScheduled: Boolean): LevelState =
      copy(collapseSmallSegmentsTaskScheduled = collapseSmallSegmentsTaskScheduled)

    override def setTask(expireKeyValueScheduled: TimerTask): LevelState =
      copy(task = Some(expireKeyValueScheduled))

    override def clearTask(): LevelState =
      copy(task = None)
  }
}

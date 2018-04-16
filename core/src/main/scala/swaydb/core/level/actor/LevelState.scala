/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import swaydb.core.actor.ActorRef
import swaydb.core.level.actor.LevelCommand.Pull
import swaydb.core.segment.Segment

private[core] sealed trait LevelState {

  val hasSmallSegments: Boolean

  def busySegments: List[Segment]

  def isSleeping: Boolean

  def isPushing: Boolean

  def isScheduled: Boolean

  val waitingPull: Option[ActorRef[Pull]]

  def copyWithHasSmallSegments(hasSmallSegments: Boolean): LevelState
}

private[core] object LevelState {
  case class Pushing(busySegments: List[Segment],
                     hasSmallSegments: Boolean,
                     waitingPull: Option[ActorRef[Pull]]) extends LevelState {
    override def isSleeping: Boolean = false

    override def isPushing: Boolean = true

    override def isScheduled: Boolean = false

    override def copyWithHasSmallSegments(hasSmallSegments: Boolean): LevelState =
      copy(hasSmallSegments = hasSmallSegments)
  }

  case class PushScheduled(hasSmallSegments: Boolean) extends LevelState {
    override def isSleeping: Boolean = false

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = true

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def copyWithHasSmallSegments(hasSmallSegments: Boolean): LevelState =
      copy(hasSmallSegments = hasSmallSegments)
  }

  case class Sleeping(hasSmallSegments: Boolean) extends LevelState {

    override def isSleeping: Boolean = true

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = false

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def copyWithHasSmallSegments(hasSmallSegments: Boolean): LevelState =
      copy(hasSmallSegments = hasSmallSegments)
  }

  case class WaitingPull(hasSmallSegments: Boolean) extends LevelState {

    override def isSleeping: Boolean = true

    override def isPushing: Boolean = false

    override def isScheduled: Boolean = false

    override def busySegments: List[Segment] = List.empty

    override val waitingPull: Option[ActorRef[Pull]] = None

    override def copyWithHasSmallSegments(hasSmallSegments: Boolean): LevelState =
      copy(hasSmallSegments = hasSmallSegments)
  }
}

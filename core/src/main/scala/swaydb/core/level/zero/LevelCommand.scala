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

package swaydb.core.level.zero

import swaydb.core.actor.ActorRef
import swaydb.core.data.Memory
import swaydb.core.map.Map
import swaydb.data.IO
import swaydb.data.slice.Slice

//Commands that Levels use for communication
sealed trait LevelZeroCommand
sealed trait LevelZeroAPI extends LevelZeroCommand
sealed trait LevelZeroResponse extends LevelZeroCommand

object LevelCommand {

  case class PushMapResponse(request: PushMap,
                             result: IO[Unit]) extends LevelZeroResponse

  sealed trait Push extends LevelZeroCommand
  case object Push extends Push

  case object WakeUp extends LevelZeroAPI

  sealed trait Pull extends LevelZeroResponse
  case object Pull extends Pull

  case class PushMap(map: Map[Slice[Byte], Memory.SegmentResponse],
                     replyTo: ActorRef[PushMapResponse])
}
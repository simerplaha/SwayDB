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
import swaydb.core.data.Memory
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline
import scala.util.Try

sealed trait LevelCommand
sealed trait LevelZeroCommand

sealed trait LevelRequest extends LevelCommand
sealed trait LevelZeroRequest extends LevelZeroCommand

//Commands that Levels use for communication
sealed trait LevelAPI extends LevelRequest
sealed trait LevelZeroAPI extends LevelZeroRequest

sealed trait LevelResponse extends LevelCommand
sealed trait LevelZeroResponse extends LevelZeroCommand

object LevelCommand {

  case class PushSegmentsResponse(request: PushSegments,
                                  result: Try[Unit]) extends LevelResponse

  case class PushMapResponse(request: PushMap,
                             result: Try[Unit]) extends LevelZeroResponse

  sealed trait Push extends LevelCommand with LevelZeroCommand
  case object Push extends Push

  case object WakeUp extends LevelCommand with LevelZeroAPI

  sealed trait Pull extends LevelResponse with LevelZeroResponse
  case object Pull extends Pull

  sealed trait CollapseSmallSegments extends LevelCommand
  case object CollapseSmallSegments extends CollapseSmallSegments

  sealed trait CollapseSmallSegmentsForce extends LevelCommand
  case object CollapseSmallSegmentsForce extends CollapseSmallSegmentsForce

  case class ClearExpiredKeyValues(nextDeadline: Deadline) extends LevelCommand

  case class PushSegments(segments: Iterable[Segment],
                          replyTo: ActorRef[PushSegmentsResponse]) extends LevelAPI

  case class PullRequest(pullFrom: ActorRef[Pull]) extends LevelAPI

  case class PushMap(map: Map[Slice[Byte], Memory.SegmentResponse],
                     replyTo: ActorRef[PushMapResponse]) extends LevelAPI
}
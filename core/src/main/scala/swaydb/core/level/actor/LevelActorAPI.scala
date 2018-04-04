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

import swaydb.core.data.{Memory, Value}
import swaydb.core.level.PathsDistributor
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

/**
  * These API should only be used by the [[LevelActor]] of that Level only.
  *
  * Inter level communication should happen via sending actor messages ([[LevelAPI]]).
  */
private[core] trait LevelActorAPI {

  def paths: PathsDistributor

  val pushForward: Boolean

  def isEmpty: Boolean

  def nextBatchSize: Int

  def hasNextLevel: Boolean

  def forward(levelAPI: LevelAPI): Try[Unit]

  def push(levelAPI: LevelAPI): Unit

  def nextPushDelayAndSegmentsCount: (FiniteDuration, Int)

  def nextBatchSizeAndSegmentsCount: (Int, Int)

  def nextPushDelayAndBatchSize: Throttle

  def nextPushDelay: FiniteDuration

  def removeSegments(segments: Iterable[Segment]): Try[Int]

  def putMap(map: Map[Slice[Byte], Memory]): Try[Unit]

  def put(segments: Iterable[Segment]): Try[Unit]

  def put(segment: Segment): Try[Unit]

  def take(count: Int): Slice[Segment]

  def pickSegmentsToPush(count: Int): Iterable[Segment]

  def collapseAllSmallSegments(batch: Int): Try[Int]

  def levelSize: Long

  def segmentCountAndLevelSize: (Int, Long)
}

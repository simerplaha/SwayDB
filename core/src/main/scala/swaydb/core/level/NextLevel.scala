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
 */

package swaydb.core.level

import swaydb.IO
import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.slice.{Slice, SliceOption}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

object NextLevel {

  def foreachRight[T](level: NextLevel, f: NextLevel => T): Unit = {
    level.nextLevel foreach {
      nextLevel =>
        foreachRight(nextLevel, f)
    }
    f(level)
  }

  def reverseNextLevels(level: NextLevel): ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachRight(
      level = level,
      f = level =>
        levels += level
    )
    levels
  }
}

/**
 * Levels that can have upper Levels or Levels that upper Levels can merge Segments or Maps into.
 */
trait NextLevel extends LevelRef {

  def pathDistributor: PathsDistributor

  def throttle: LevelMeter => Throttle

  def isUnreserved(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean

  def isUnreserved(segment: Segment): Boolean

  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean

  def isCopyable(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): Boolean

  def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment])

  def mightContainFunction(key: Slice[Byte]): Boolean

  def put(segment: Segment): IO[Promise[Unit], IO[swaydb.Error.Level, Set[Int]]]

  def put(map: Map[SliceOption[Byte], MemoryOption, Slice[Byte], Memory]): IO[Promise[Unit], IO[swaydb.Error.Level, Set[Int]]]

  def put(segments: Iterable[Segment]): IO[Promise[Unit], IO[swaydb.Error.Level, Set[Int]]]

  def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Level, Int]

  def meter: LevelMeter

  def refresh(segment: Segment): IO[Promise[Unit], IO[swaydb.Error.Level, Unit]]

  def collapse(segments: Iterable[Segment]): IO[Promise[Unit], IO[swaydb.Error.Level, Int]]

  def reverseNextLevels: ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachRight(
      level = this,
      f = level =>
        levels += level
    )
    levels
  }

  def levelSize: Long

  def minSegmentSize: Int

  def lastSegmentId: Option[Long]

  def take(count: Int): Slice[Segment]

  def takeSmallSegments(size: Int): Iterable[Segment]

  def takeLargeSegments(size: Int): Iterable[Segment]

  def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment])

  def optimalSegmentsToCollapse(take: Int): Iterable[Segment]

  def takeSegments(size: Int,
                   condition: Segment => Boolean): Iterable[Segment]

  def segmentsInLevel(): Iterable[Segment]

  def nextThrottlePushCount: Int

  def delete: IO[swaydb.Error.Delete, Unit]
}

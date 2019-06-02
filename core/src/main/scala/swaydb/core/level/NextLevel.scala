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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.level

import swaydb.core.data.Memory
import swaydb.core.map.Map
import swaydb.core.segment.Segment
import swaydb.data.IO
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

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

  def isUnReserved(minKey: Slice[Byte], maxKey: Slice[Byte]): Boolean

  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte]): Boolean

  def isCopyable(map: Map[Slice[Byte], Memory.SegmentResponse]): Boolean

  def partitionUnreservedCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment])

  def put(segment: Segment)(implicit ec: ExecutionContext): IO.Async[Unit]

  def put(map: Map[Slice[Byte], Memory.SegmentResponse])(implicit ec: ExecutionContext): IO.Async[Unit]

  def put(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO.Async[Unit]

  def removeSegments(segments: Iterable[Segment]): IO[Int]

  def meter: LevelMeter

  def refresh(segment: Segment)(implicit ec: ExecutionContext): IO.Async[Unit]

  def collapse(segments: Iterable[Segment])(implicit ec: ExecutionContext): IO.Async[Int]

  def reverseNextLevels: ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachRight(
      level = this,
      f = level =>
        levels += level
    )
    levels
  }
}

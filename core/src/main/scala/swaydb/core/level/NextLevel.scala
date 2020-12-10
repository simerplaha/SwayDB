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

package swaydb.core.level

import swaydb.{Error, IO}
import swaydb.core.data.Memory
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.Map
import swaydb.core.segment.format.a.block.segment.data.TransientSegment
import swaydb.core.segment.Segment
import swaydb.data.compaction.{LevelMeter, ParallelMerge, Throttle}
import swaydb.data.config.PushForwardStrategy
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}

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

  def isCopyable(minKey: Slice[Byte], maxKey: Slice[Byte], maxKeyInclusive: Boolean): Boolean

  def isCopyable(map: Map[Slice[Byte], Memory, LevelZeroMapCache]): Boolean

  def isNonEmpty(): Boolean

  def pushForwardStrategy: PushForwardStrategy

  def partitionCopyable(segments: Iterable[Segment]): (Iterable[Segment], Iterable[Segment])

  def mightContainFunction(key: Slice[Byte]): Boolean

  def put(segment: Segment,
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]]

  def put(map: Map[Slice[Byte], Memory, LevelZeroMapCache],
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]]

  def put(segments: Iterable[Segment],
          parallelMerge: ParallelMerge)(implicit ec: ExecutionContext): Future[Iterable[LevelMergeResult]]

  def removeSegments(segments: Iterable[Segment]): IO[swaydb.Error.Level, Int]

  def meter: LevelMeter

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

  def openedFiles(): Long

  def pendingDeletes(): Long

  def take(count: Int): Slice[Segment]

  def takeSmallSegments(size: Int): Iterable[Segment]

  def takeLargeSegments(size: Int): Iterable[Segment]

  def optimalSegmentsPushForward(take: Int): (Iterable[Segment], Iterable[Segment])

  def optimalSegmentsToCollapse(take: Int): Iterable[Segment]

  def takeSegments(size: Int,
                   condition: Segment => Boolean): Iterable[Segment]

  def segmentsInLevel(): Iterable[Segment]

  def nextThrottlePushCount: Int

  def deleteNoSweep: IO[swaydb.Error.Level, Unit]

  def deleteNoSweepNoClose(): IO[swaydb.Error.Level, Unit]

  def closeNoSweepNoRelease(): IO[swaydb.Error.Level, Unit]
}

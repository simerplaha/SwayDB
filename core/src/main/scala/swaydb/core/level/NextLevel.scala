/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.core.data.{DefIO, Memory}
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.merge.stats.MergeStats
import swaydb.core.segment.assigner.{Assignable, Assignment}
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.data.compaction.{LevelMeter, LevelThrottle}
import swaydb.data.slice.Slice
import swaydb.effect.Dir
import swaydb.{Error, IO}

import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

object NextLevel {

  def foreachRight[T](level: NextLevel, f: NextLevel => T): Unit = {
    level.nextLevel foreach {
      nextLevel =>
        foreachRight(nextLevel, f)
    }
    f(level)
  }

  def foreachLeft[T](level: NextLevel, f: NextLevel => T): Unit =
    level.nextLevel foreach {
      nextLevel =>
        f(nextLevel)
        foreachLeft(nextLevel, f)
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

  def dirs: Seq[Dir]

  def pathDistributor: PathsDistributor

  def throttle: LevelMeter => LevelThrottle

  def isNonEmpty(): Boolean

  def mightContainFunction(key: Slice[Byte]): Boolean

  def assign(newKeyValues: Assignable.Collection,
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]

  def assign(newKeyValues: Iterable[Assignable.Collection],
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]

  def assign(newKeyValues: LevelZeroMap,
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]

  def merge(assigment: DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]],
            removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                           compactionIO: CompactionIO.Actor): Future[Iterable[DefIO[SegmentOption, Iterable[Segment]]]]

  def refresh(segment: Iterable[Segment],
              removeDeletedRecords: Boolean): IO[Error.Level, Iterable[DefIO[Segment, Slice[TransientSegment]]]]

  def collapse(segments: Iterable[Segment],
               removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                              compactionIO: CompactionIO.Actor): Future[LevelCollapseResult]

  def commit(mergeResult: DefIO[SegmentOption, Iterable[TransientSegment]]): IO[Error.Level, Unit]

  def commit(mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit]

  def commit(collapsed: LevelCollapseResult.Collapsed): IO[Error.Level, Unit]

  def commit(old: Iterable[Segment],
             merged: Iterable[DefIO[SegmentOption, Iterable[Segment]]]): IO[Error.Level, Unit]

  def remove(segments: Iterable[Segment]): IO[swaydb.Error.Level, Unit]

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

  def nextLevels: ListBuffer[NextLevel] = {
    val levels = ListBuffer.empty[NextLevel]
    NextLevel.foreachLeft(
      level = this,
      f = level => levels += level
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

  def takeSegments(size: Int,
                   condition: Segment => Boolean): Iterable[Segment]

  def segments(): Iterable[Segment]

  def deleteNoSweep: IO[swaydb.Error.Level, Unit]

  def deleteNoSweepNoClose(): IO[swaydb.Error.Level, Unit]

  def closeNoSweepNoRelease(): IO[swaydb.Error.Level, Unit]
}

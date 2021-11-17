/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.level

import swaydb.config.compaction.CompactionConfig.CompactionParallelism
import swaydb.config.compaction.{LevelMeter, LevelThrottle}
import swaydb.core.compaction.io.CompactionIO
import swaydb.core.segment.data.Memory
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.assigner.{Assignable, Assignment}
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.DefIO
import swaydb.effect.Dir
import swaydb.slice.Slice
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

  def assign(newKeyValues: LevelZeroLog,
             targetSegments: IterableOnce[Segment],
             removeDeletedRecords: Boolean): DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]]

  def merge(assigment: DefIO[Iterable[Assignable], Iterable[Assignment[Iterable[Assignable.Gap[MergeStats.Segment[Memory, ListBuffer]]], ListBuffer[Assignable], Segment]]],
            removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                           compactionIO: CompactionIO.Actor,
                                           parallelism: CompactionParallelism): Future[Iterable[DefIO[SegmentOption, Iterable[Segment]]]]

  def refresh(segment: Iterable[Segment],
              removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                             parallelism: CompactionParallelism): Future[Iterable[DefIO[Segment, Slice[TransientSegment]]]]

  def collapse(segments: Iterable[Segment],
               removeDeletedRecords: Boolean)(implicit ec: ExecutionContext,
                                              compactionIO: CompactionIO.Actor,
                                              parallelism: CompactionParallelism): Future[LevelCollapseResult]

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

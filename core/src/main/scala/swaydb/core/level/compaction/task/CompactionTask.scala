/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.task

import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable

sealed trait CompactionTask {
  def targetLevel: LevelRef
}

/**
 * Compaction tasks that can be submitted to
 * [[swaydb.core.level.compaction.committer.CompactionCommitter]]
 */
object CompactionTask {

  sealed trait Segments extends CompactionTask

  /**
   * Compaction task which is executed to perform compaction.
   *
   * @param targetLevel [[Level]] where the [[data]] should be written to.
   * @param data        data to merge.
   */
  case class Task[A <: Assignable.Collection](targetLevel: Level,
                                              data: Iterable[A])

  case class CompactSegments(targetLevel: Level,
                             tasks: Iterable[Task[Segment]]) extends CompactionTask.Segments

  case class CollapseSegments(targetLevel: Level,
                              segments: Iterable[Segment]) extends CompactionTask.Segments

  case class RefreshSegments(targetLevel: Level,
                             segments: Iterable[Segment]) extends CompactionTask.Segments

  case class CompactMaps(targetLevel: LevelZero,
                         maps: Iterable[LevelZeroMap],
                         tasks: Iterable[Task[Assignable.Collection]]) extends CompactionTask

}

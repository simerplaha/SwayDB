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
  def source: LevelRef
}

/**
 * Compaction tasks that can be submitted to
 * [[swaydb.core.level.compaction.throttle.behaviour.BehaviourCompactionTask]]
 * for execution.
 */
object CompactionTask {

  sealed trait Segments extends CompactionTask
  sealed trait Cleanup extends Segments

  /**
   * Compaction task which is executed to perform compaction.
   *
   * @param target [[Level]] where the [[data]] should be written to.
   * @param data   data to merge.
   */
  case class Task[A <: Assignable.Collection](target: Level,
                                              data: scala.collection.SortedSet[A])

  case class CompactSegments(source: Level,
                             tasks: Iterable[Task[Segment]]) extends CompactionTask.Segments

  /**
   * @param maps should be in the same order at it's position in [[LevelZero]].
   */
  case class CompactMaps(source: LevelZero,
                         maps: Iterator[LevelZeroMap],
                         tasks: Iterable[Task[Assignable.Collection]]) extends CompactionTask

  case class CollapseSegments(source: Level,
                              segments: Iterable[Segment]) extends Cleanup

  case class RefreshSegments(source: Level,
                             segments: Iterable[Segment]) extends Cleanup

}

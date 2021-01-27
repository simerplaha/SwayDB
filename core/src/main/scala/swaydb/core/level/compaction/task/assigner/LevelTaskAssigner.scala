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

package swaydb.core.level.compaction.task.assigner

import swaydb.core.level.Level
import swaydb.core.level.compaction.task.CompactionDataType._
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.data.NonEmptyList
import swaydb.data.compaction.PushStrategy
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

case object LevelTaskAssigner {

  /**
   * @return optimal [[Segment]]s to compact to
   *         control the overflow.
   */
  def assign(source: Level,
             pushStrategy: PushStrategy,
             lowerLevels: NonEmptyList[Level],
             sourceOverflow: Long): CompactionTask.CompactSegments = {
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder

    val tasks =
      TaskAssigner.assignQuick[Segment](
        data = source.segments(),
        lowerLevels = lowerLevels,
        dataOverflow = sourceOverflow,
        pushStrategy = pushStrategy
      )

    CompactionTask.CompactSegments(source, tasks)
  }

  def cleanup(level: Level): Option[CompactionTask.Cleanup] =
    refresh(level) orElse collapse(level)

  def refresh(level: Level): Option[CompactionTask.RefreshSegments] = {
    val segments =
      level
        .segments()
        .filter(_.nearestPutDeadline.exists(!_.hasTimeLeft()))

    if (segments.isEmpty)
      None
    else
      Some(CompactionTask.RefreshSegments(level, segments))
  }

  def collapse(level: Level): Option[CompactionTask.CollapseSegments] = {
    var segmentsCount = 0

    val segments =
      level
        .segments()
        .filter {
          segment =>
            segmentsCount += 1
            Level.isSmallSegment(segment, level.minSegmentSize)
        }

    //do not collapse if no small Segments or if there is only 2 Segments in the Level
    if (segments.isEmpty || segmentsCount <= 2)
      None
    else
      Some(CompactionTask.CollapseSegments(level, segments))
  }
}

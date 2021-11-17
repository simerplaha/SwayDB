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

package swaydb.core.compaction.task.assigner

import swaydb.core.level.Level
import swaydb.core.compaction.task.CompactionDataType._
import swaydb.core.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.utils.NonEmptyList
import swaydb.config.compaction.PushStrategy
import swaydb.slice.order.KeyOrder
import swaydb.slice.Slice

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
            Level.shouldCollapse(level, segment)
        }

    //do not collapse if no small Segments or if there is only 2 Segments in the Level
    if (segments.isEmpty || segmentsCount <= 2)
      None
    else
      Some(CompactionTask.CollapseSegments(level, segments))
  }
}

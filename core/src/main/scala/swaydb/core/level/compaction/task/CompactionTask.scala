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

package swaydb.core.level.compaction.task

import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.level.{Level, LevelRef}
import swaydb.core.segment.Segment
import swaydb.core.segment.assigner.Assignable

import scala.collection.mutable.ListBuffer

sealed trait CompactionTask {
  def source: LevelRef

  def compactingLevels: Iterable[LevelRef]
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
                             tasks: Iterable[Task[Segment]]) extends CompactionTask.Segments {
    override def compactingLevels: Iterable[Level] = {
      val buffer = ListBuffer.empty[Level]
      buffer += source
      tasks foreach {
        task =>
          buffer += task.target
      }
      buffer
    }
  }

  /**
   * @param maps Should not be in random order.
   *             Should be in the same order at it's position in [[LevelZero]].
   */
  case class CompactMaps(source: LevelZero,
                         maps: List[LevelZeroMap],
                         tasks: Iterable[Task[Assignable.Collection]]) extends CompactionTask {
    override def compactingLevels: Iterable[LevelRef] = {
      val buffer = ListBuffer.empty[LevelRef]
      buffer += source
      tasks foreach {
        task =>
          buffer += task.target
      }
      buffer
    }
  }

  case class CollapseSegments(source: Level,
                              segments: Iterable[Segment]) extends Cleanup {
    override def compactingLevels: Iterable[Level] =
      Iterable(source)
  }

  case class RefreshSegments(source: Level,
                             segments: Iterable[Segment]) extends Cleanup {
    override def compactingLevels: Iterable[Level] =
      Iterable(source)
  }
}

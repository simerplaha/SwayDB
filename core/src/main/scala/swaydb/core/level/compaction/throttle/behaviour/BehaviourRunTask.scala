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

package swaydb.core.level.compaction.throttle.behaviour

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level._
import swaydb.core.level.compaction.task.CompactionTask

import scala.concurrent.{ExecutionContext, Future}

/**
 * Implements compaction functions.
 */
private[throttle] object BehaviourRunTask extends LazyLogging {

  private[throttle] def runSegmentTask(task: CompactionTask.Segments,
                                       lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        runSegmentTask(task = task, lockedLastLevel = lockedLastLevel)

      case task: CompactionTask.Cleanup =>
        runCleanupTask(task = task, lockedLastLevel = lockedLastLevel)
    }

  private[throttle] def runCleanupTask(task: CompactionTask.Cleanup,
                                       lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    task match {
      case task: CompactionTask.CollapseSegments =>
        runSegmentTask(task = task, lockedLastLevel = lockedLastLevel)

      case task: CompactionTask.RefreshSegments =>
        runSegmentTask(task = task, lockedLastLevel = lockedLastLevel)
    }

  private[throttle] def runSegmentTask(task: CompactionTask.CompactSegments,
                                       lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          Future {
            task.target.assign(
              newKeyValues = task.data,
              targetSegments = task.target.segments(),
              removeDeletedRecords = task.target.levelNumber == lockedLastLevel.levelNumber
            )
          } flatMap {
            levelAssignment =>
              task.target.merge(levelAssignment)
          } map {
            result =>
              (task.target, result)
          }
      } flatMap {
        result =>
          BehaviourCommit.commit(
            fromLevel = task.source,
            segments = task.tasks.flatMap(_.data),
            mergeResults = result
          ).toFuture
      }

  private[throttle] def runSegmentTask(task: CompactionTask.CollapseSegments,
                                       lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .source
        .collapse(segments = task.segments, removeDeletedRecords = task.source.levelNumber == lockedLastLevel.levelNumber)
        .flatMap {
          case LevelCollapseResult.Empty =>
            Future.failed(new Exception(s"Collapse failed: ${LevelCollapseResult.productPrefix}.${LevelCollapseResult.Empty.productPrefix}"))

          case LevelCollapseResult.Collapsed(sourceSegments, mergeResult) =>
            BehaviourCommit.replace(
              level = task.source,
              old = sourceSegments,
              result = mergeResult
            ).toFuture
        }

  private[throttle] def runSegmentTask(task: CompactionTask.RefreshSegments,
                                       lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .source
        .refresh(segments = task.segments, removeDeletedRecords = task.source.levelNumber == lockedLastLevel.levelNumber)
        .toFuture //execute on current thread.
        .flatMap {
          result =>
            BehaviourCommit.commit(
              level = task.source,
              result = result
            ).toFuture
        }

  private[throttle] def runMapTask(task: CompactionTask.CompactMaps,
                                   lockedLastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.maps.isEmpty)
      Future.unit
    else
      Future.traverse(task.tasks) {
        task =>
          Future {
            task.target.assign(
              newKeyValues = task.data,
              targetSegments = task.target.segments(),
              removeDeletedRecords = task.target.levelNumber == lockedLastLevel.levelNumber
            )
          } flatMap {
            levelAssignment =>
              task.target.merge(levelAssignment)
          } map {
            result =>
              (task.target, result)
          }
      } flatMap {
        result =>
          BehaviourCommit.commit(
            fromLevel = task.source,
            maps = task.maps,
            mergeResults = result
          ).toFuture
      }
}

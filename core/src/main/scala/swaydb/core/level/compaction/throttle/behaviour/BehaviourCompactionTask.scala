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
import swaydb.core.data.DefIO
import swaydb.core.level._
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.SegmentOption
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.{Error, IO}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Implements compaction functions.
 */
protected object BehaviourCompactionTask extends LazyLogging {

  def runSegmentTask(task: CompactionTask.Segments,
                     lastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        runCompactSegments(task = task, lastLevel = lastLevel)

      case task: CompactionTask.Cleanup =>
        runCleanupTask(task = task, lastLevel = lastLevel)
    }

  def runCleanupTask(task: CompactionTask.Cleanup,
                     lastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    task match {
      case task: CompactionTask.CollapseSegments =>
        runCollapse(task = task, lastLevel = lastLevel)

      case task: CompactionTask.RefreshSegments =>
        //Runs on current thread because these functions are already
        //being invoked by compaction thread and there is no concurrency
        //required when running refresh.
        runRefresh(task = task, lastLevel = lastLevel).toFuture
    }

  def runTasks[A <: Assignable.Collection](tasks: Iterable[CompactionTask.Task[A]],
                                           lastLevel: Level)(implicit ec: ExecutionContext): Future[Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]] =
    Future.traverse(tasks) {
      task =>
        val removeDeletedRecords = task.target.levelNumber == lastLevel.levelNumber

        Future {
          task.target.assign(
            newKeyValues = task.data,
            targetSegments = task.target.segments(),
            removeDeletedRecords = removeDeletedRecords
          )
        } flatMap {
          levelAssignment =>
            task.target.merge(
              assigment = levelAssignment,
              removeDeletedRecords = removeDeletedRecords
            )
        } map {
          mergeResult =>
            DefIO(
              input = task.target,
              output = mergeResult
            )
        }
    }

  def runCompactSegments(task: CompactionTask.CompactSegments,
                         lastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      runTasks(
        tasks = task.tasks,
        lastLevel = lastLevel
      ) flatMap {
        mergeResult =>
          BehaviourCommit.commit(
            fromLevel = task.source,
            segments = task.tasks.flatMap(_.data),
            mergeResults = mergeResult
          ).toFuture
      }


  def runCompactMaps(task: CompactionTask.CompactMaps,
                     lastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.maps.isEmpty)
      Future.unit
    else
      runTasks(
        tasks = task.tasks,
        lastLevel = lastLevel
      ) flatMap {
        result =>
          BehaviourCommit.commit(
            fromLevel = task.source,
            maps = task.maps,
            mergeResults = result
          ).toFuture
      }

  def runCollapse(task: CompactionTask.CollapseSegments,
                  lastLevel: Level)(implicit ec: ExecutionContext): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      task
        .source
        .collapse(
          segments = task.segments,
          removeDeletedRecords = task.source.levelNumber == lastLevel.levelNumber
        )
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

  def runRefresh(task: CompactionTask.RefreshSegments,
                 lastLevel: Level): IO[Error.Level, Unit] =
    if (task.segments.isEmpty)
      IO.unit
    else
      task
        .source
        .refresh(
          segments = task.segments,
          removeDeletedRecords = task.source.levelNumber == lastLevel.levelNumber
        )
        .flatMap {
          result =>
            BehaviourCommit.commit(
              level = task.source,
              result = result
            )
        }
}

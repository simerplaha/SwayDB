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
import swaydb.DefActor
import swaydb.core.data.DefIO
import swaydb.core.level._
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.sweeper.FileSweeper
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.slice.Slice
import swaydb.utils.Futures
import swaydb.utils.Futures.FutureImplicits

import scala.concurrent.{ExecutionContext, Future}

/**
 * Implements compaction functions.
 */
protected object BehaviourCompactionTask extends LazyLogging {

  def runSegmentTask(task: CompactionTask.Segments,
                     lastLevel: Level)(implicit ec: ExecutionContext,
                                       fileSweeper: FileSweeper.On,
                                       parallelism: CompactionParallelism): Future[Unit] =
    task match {
      case task: CompactionTask.CompactSegments =>
        compactSegments(task = task, lastLevel = lastLevel)

      case task: CompactionTask.Cleanup =>
        runCleanupTask(task = task, lastLevel = lastLevel)
    }

  def runCleanupTask(task: CompactionTask.Cleanup,
                     lastLevel: Level)(implicit ec: ExecutionContext,
                                       fileSweeper: FileSweeper.On,
                                       parallelism: CompactionParallelism): Future[Unit] =
    task match {
      case task: CompactionTask.CollapseSegments =>
        collapse(task = task, lastLevel = lastLevel)

      case task: CompactionTask.RefreshSegments =>
        //Runs on current thread because these functions are already
        //being invoked by compaction thread and there is no concurrency
        //required when running refresh.
        refresh(task = task, lastLevel = lastLevel)
    }

  @inline def ensurePauseSweeper[T](levels: Iterable[LevelRef])(compact: => Future[T])(implicit fileSweeper: FileSweeper.On,
                                                                                       ec: ExecutionContext): Future[T] = {
    //No need to wait for a response because FileSweeper's queue is ordered prioritising PauseResume messages.
    //Who not? Because the Actor is configurable which could be a cached timer with longer time interval
    //which means we might not get a response immediately.
    fileSweeper.closer.send(FileSweeper.Command.Pause(levels))
    compact.unitCallback(fileSweeper.send(FileSweeper.Command.Resume(levels)))
  }

  private def runTasks[A <: Assignable.Collection](tasks: Iterable[CompactionTask.Task[A]],
                                                   lastLevel: Level)(implicit ec: ExecutionContext,
                                                                     fileSweeper: FileSweeper.On,
                                                                     parallelism: CompactionParallelism): Future[Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]] = {
    implicit val compactionIO: CompactionIO.Actor = CompactionIO.create()

    Futures.traverseBounded(parallelism.multiLevelTaskParallelism, tasks) {
      task =>
        val removeDeletedRecords = task.target.levelNumber == lastLevel.levelNumber

        Future {
          task.target.assign(
            newKeyValues = task.data,
            targetSegments = task.target.segments(),
            removeDeletedRecords = removeDeletedRecords
          )
        } flatMap {
          assignment =>
            task.target.merge(
              assigment = assignment,
              removeDeletedRecords = removeDeletedRecords
            )
        } map {
          mergeResult =>
            DefIO(
              input = task.target,
              output = mergeResult
            )
        }
    }.flatMapCallback(compactionIO.terminate[Future]())
  }

  def compactSegments(task: CompactionTask.CompactSegments,
                      lastLevel: Level)(implicit ec: ExecutionContext,
                                        fileSweeper: FileSweeper.On,
                                        parallelism: CompactionParallelism): Future[Unit] =
    if (task.tasks.isEmpty)
      Future.unit
    else
      ensurePauseSweeper(task.compactingLevels) {
        runTasks(
          tasks = task.tasks,
          lastLevel = lastLevel
        ) flatMap {
          mergeResult =>
            BehaviourCommit.commitPersistedSegments(
              fromLevel = task.source,
              segments = task.tasks.flatMap(_.data),
              persisted = Slice.from(mergeResult, mergeResult.size)
            ).toFuture
        }
      }

  def compactMaps(task: CompactionTask.CompactMaps,
                  lastLevel: Level)(implicit ec: ExecutionContext,
                                    fileSweeper: FileSweeper.On,
                                    parallelism: CompactionParallelism): Future[Unit] =
    if (task.maps.isEmpty)
      Future.unit
    else
      ensurePauseSweeper(task.compactingLevels) {
        runTasks(
          tasks = task.tasks,
          lastLevel = lastLevel
        ) flatMap {
          result =>
            BehaviourCommit.commitPersistedMaps(
              fromLevel = task.source,
              maps = task.maps,
              mergeResults = Slice.from(result, result.size)
            ).toFuture
        }
      }

  def collapse(task: CompactionTask.CollapseSegments,
               lastLevel: Level)(implicit ec: ExecutionContext,
                                 fileSweeper: FileSweeper.On,
                                 parallelism: CompactionParallelism): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      ensurePauseSweeper(task.compactingLevels) {
        implicit val compactionIO: CompactionIO.Actor = CompactionIO.create()

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
          .flatMapCallback(compactionIO.terminate[Future]())
      }

  def refresh(task: CompactionTask.RefreshSegments,
              lastLevel: Level)(implicit fileSweeper: FileSweeper.On,
                                ec: ExecutionContext,
                                parallelism: CompactionParallelism): Future[Unit] =
    if (task.segments.isEmpty)
      Future.unit
    else
      ensurePauseSweeper(task.compactingLevels) {
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
              ).toFuture
          }
      }
}

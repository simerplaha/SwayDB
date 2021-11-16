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

package swaydb.core.level.compaction.throttle.behaviour

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.DefIO
import swaydb.core.level._
import swaydb.core.level.compaction.io.CompactionIO
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.assigner.Assignable
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.sweeper.FileSweeper
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.slice.Slice
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

  def compactLogs(task: CompactionTask.CompactLogs,
                  lastLevel: Level)(implicit ec: ExecutionContext,
                                    fileSweeper: FileSweeper.On,
                                    parallelism: CompactionParallelism): Future[Unit] =
    if (task.logs.isEmpty)
      Future.unit
    else
      ensurePauseSweeper(task.compactingLevels) {
        runTasks(
          tasks = task.tasks,
          lastLevel = lastLevel
        ) flatMap {
          result =>
            BehaviourCommit.commitPersistedLogs(
              fromLevel = task.source,
              logs = task.logs,
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

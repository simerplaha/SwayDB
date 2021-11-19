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

package swaydb.core.compaction.throttle.behaviour

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroLog
import swaydb.core.segment.block.segment.transient.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.DefIO
import swaydb.slice.Slice
import swaydb.SliceIOImplicits._
import swaydb.{Error, IO}

protected case object BehaviourCommit extends LazyLogging {

  private val levelCommitOrder =
    Ordering.Int.reverse.on[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]](_.input.levelNumber)

  def persist(mergeResults: Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]): IO[Error.Level, Slice[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]] =
    mergeResults
      .mapRecoverIO[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]](
        block = {
          levelIO =>
            levelIO
              .input
              .persist(levelIO.output)
              .transform(levelIO.withOutput)
        },
        recover = {
          case (result, error) =>
            logger.error(s"Failed to create Segments. Performing cleanup.", error.exception)
            result foreach {
              levelIO =>
                levelIO.output foreach {
                  segmentIO =>
                    segmentIO.output foreach {
                      segment =>
                        IO(segment.delete) onLeftSideEffect {
                          exception =>
                            logger.error(s"Failed to delete Segment ${segment.path}", exception)
                        }
                    }
                }
            }
        }
      )

  def commit(persisted: Slice[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]): IO[Error.Level, Slice[Unit]] =
  //commit the Segments to each Level in reverse order
    persisted.sorted(levelCommitOrder) mapRecoverIO {
      defIO =>
        defIO.input.commitPersisted(defIO.output)
    }

  def persistCommit(mergeResults: Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]): IO[Error.Level, Slice[Unit]] =
    persist(mergeResults).flatMap(commit)

  def commit(fromLevel: Level,
             segments: Iterable[Segment],
             toLevel: Level,
             mergeResult: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    toLevel
      .commit(mergeResult)
      .and(fromLevel.remove(segments))

  def persistCommitSegments(fromLevel: Level,
                            segments: Iterable[Segment],
                            mergeResults: Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]): IO[Error.Level, Unit] =
    persistCommit(mergeResults)
      .and(fromLevel.remove(segments))

  def commitPersistedSegments(fromLevel: Level,
                              segments: Iterable[Segment],
                              persisted: Slice[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]): IO[Error.Level, Unit] =
    commit(persisted)
      .and(fromLevel.remove(segments))

  def persistCommitLogs(fromLevel: LevelZero,
                        logs: List[LevelZeroLog],
                        mergeResults: Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]): IO[Error.Level, Unit] =
    persistCommit(mergeResults)
      .and {
        logs
          .reverse
          .mapRecoverIO {
            log =>
              fromLevel
                .logs
                .removeLast(log)
          }.transform(_ => ())
      }

  def commitPersistedLogs(fromLevel: LevelZero,
                          logs: List[LevelZeroLog],
                          mergeResults: Slice[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]): IO[Error.Level, Unit] =
    commit(mergeResults)
      .and {
        logs
          .reverse
          .mapRecoverIO {
            log =>
              fromLevel.logs.removeLast(log)
          }.transform(_ => ())
      }

  def commit(level: Level,
             result: Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    level.commit(result)

  def replace(level: Level,
              old: Iterable[Segment],
              result: Iterable[DefIO[SegmentOption, Iterable[Segment]]]): IO[Error.Level, Unit] =
    level.commit(
      old = old,
      persisted = result
    )
}

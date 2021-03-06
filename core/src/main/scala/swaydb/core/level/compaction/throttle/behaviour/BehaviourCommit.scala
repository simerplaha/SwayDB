/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.data.DefIO
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.data.slice.Slice
import swaydb.data.slice.SliceIOImplicits._
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

  def persistCommitMaps(fromLevel: LevelZero,
                        maps: List[LevelZeroMap],
                        mergeResults: Iterable[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[TransientSegment]]]]]): IO[Error.Level, Unit] =
    persistCommit(mergeResults)
      .and {
        maps
          .reverse
          .mapRecoverIO {
            map =>
              fromLevel
                .maps
                .removeLast(map)
          }.transform(_ => ())
      }

  def commitPersistedMaps(fromLevel: LevelZero,
                          maps: List[LevelZeroMap],
                          mergeResults: Slice[DefIO[Level, Iterable[DefIO[SegmentOption, Iterable[Segment]]]]]): IO[Error.Level, Unit] =
    commit(mergeResults)
      .and {
        maps
          .reverse
          .mapRecoverIO {
            map =>
              fromLevel.maps.removeLast(map)
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

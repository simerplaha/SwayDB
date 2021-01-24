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
import swaydb.IO._
import swaydb.core.level.Level
import swaydb.core.level.compaction.CompactResult
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.zero.LevelZero.LevelZeroMap
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

import scala.collection.compat._

case object BehaviourCommit extends LazyLogging {

  private def commitMergeResult(mergeResults: Iterable[(Level, Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]])]): IO[Error.Level, Slice[Unit]] =
    mergeResults
      .mapRecoverIO[(Level, Iterable[CompactResult[SegmentOption, Iterable[Segment]]])](
        block = {
          case (level, result) =>
            level
              .persist(result)
              .transform(result => (level, result))
        },
        recover = {
          case (result, error) =>
            logger.error(s"Failed to create Segments. Performing cleanup.", error.exception)
            result foreach {
              case (_, result) =>
                result foreach {
                  result =>
                    result.result foreach {
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
      .flatMap {
        persisted =>
          //TODO - reverse?
          persisted.mapRecoverIO {
            case (level, persistedResult) =>
              level.commitPersisted(persistedResult, None)
          }
      }

  def commit(fromLevel: Level,
             segments: Iterable[Segment],
             toLevel: Level,
             mergeResult: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    toLevel
      .commit(mergeResult)
      .and(fromLevel.remove(segments))

  def commit(fromLevel: Level,
             segments: Iterable[Segment],
             mergeResults: Iterable[(Level, Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]])]): IO[Error.Level, Unit] =
    commitMergeResult(mergeResults)
      .and(fromLevel.remove(segments))

  def commit(fromLevel: LevelZero,
             maps: IterableOnce[LevelZeroMap],
             mergeResults: Iterable[(Level, Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]])]): IO[Error.Level, Unit] =
    commitMergeResult(mergeResults)
      .and {
        maps
          .iterator
          .toList
          .reverse
          .mapRecoverIO {
            map =>
              fromLevel.maps.removeLast(map)
          }.transform(_ => ())
      }

  def commit(level: Level,
             result: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    level.commit(result)

  def replace(level: Level,
              old: Iterable[Segment],
              result: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): IO[Error.Level, Unit] =
    level.commit(
      old = old,
      merged = result
    )
}

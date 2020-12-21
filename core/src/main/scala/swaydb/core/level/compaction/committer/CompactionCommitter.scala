/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.committer

import swaydb.core.data.MergeResult
import swaydb.core.level.NextLevel
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.data.slice.Slice
import swaydb.{Actor, ActorWire}

import scala.concurrent.{ExecutionContext, Future}

sealed trait CompactionCommitter {
  def productPrefix: String

  def commit(fromLevel: NextLevel,
             toLevel: NextLevel,
             mergeResult: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

  def commit(fromLevel: LevelZero,
             toLevel: NextLevel,
             mergeResult: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

  def replace(level: NextLevel,
              old: Segment,
              neu: Slice[TransientSegment]): Future[Unit]

  def replace(level: NextLevel,
              old: Iterable[Segment],
              neu: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]
}

case object CompactionCommitter {

  def createActor[C <: CompactionCommitter](ec: ExecutionContext)(implicit compactionCommitter: C): ActorWire[C, Unit] =
    Actor.wire[C, Unit](
      name = compactionCommitter.productPrefix,
      impl = compactionCommitter,
      state = ()
    )(ec)

  implicit case object DefaultCommitter extends CompactionCommitter {

    def commit(fromLevel: NextLevel,
               toLevel: NextLevel,
               mergeResult: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit] =
      ???

    override def commit(fromLevel: LevelZero,
                        toLevel: NextLevel,
                        mergeResult: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit] =
      ???

    override def replace(level: NextLevel,
                         old: Segment,
                         neu: Slice[TransientSegment]): Future[Unit] = ???

    override def replace(level: NextLevel,
                         old: Iterable[Segment],
                         neu: Iterable[MergeResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit] =
      ???
  }
}

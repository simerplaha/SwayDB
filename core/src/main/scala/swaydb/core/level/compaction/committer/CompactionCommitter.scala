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

import swaydb.core.level.NextLevel
import swaydb.core.level.compaction.CompactResult
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.block.segment.data.TransientSegment
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.{Actor, ActorWire}

import scala.concurrent.{ExecutionContext, Future}

trait CompactionCommitter {

  def name: String

  def commit(fromLevel: NextLevel,
             segments: Iterable[Segment],
             toLevel: NextLevel,
             mergeResult: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

  def commit(fromLevel: LevelZero,
             toLevel: NextLevel,
             mergeResult: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

  def commit(level: NextLevel,
             result: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

  def replace(level: NextLevel,
              old: Iterable[Segment],
              result: Iterable[CompactResult[SegmentOption, Iterable[TransientSegment]]]): Future[Unit]

}

case object CompactionCommitter {

  def createActor(ec: ExecutionContext): ActorWire[CompactionCommitter, Unit] =
    Actor.wire[CompactionCommitter, Unit](
      name = CurrentThreadCompactionCommitter.name,
      impl = CurrentThreadCompactionCommitter,
      state = ()
    )(ec)

}

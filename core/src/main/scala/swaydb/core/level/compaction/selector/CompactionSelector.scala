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

package swaydb.core.level.compaction.selector

import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.core.segment.Segment
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise

case object CompactionSelector {

  @inline def select(source: Level,
                     take: Int): Either[Promise[Unit], CompactionTask.Segments] =
    ???

  @inline def select(source: LevelZero): Either[Promise[Unit], CompactionTask.CompactMaps] =
    ???

  private def select(source: Level,
                     nextLevel: Level,
                     take: Int): Either[Promise[Unit], CompactionTask.Segments] = {
    val segmentsToMerge = ListBuffer.empty[(Int, Segment)]
    implicit val keyOrder: KeyOrder[Slice[Byte]] = source.keyOrder

    //    level
    //      .segments()
    //      .foreachBreak {
    //        segment =>
    //          if (level.isUnreserved(segment) && nextLevel.isUnreserved(segment)) {
    //            val count = Segment.overlapsCount(segment, nextLevel.segments())
    //
    //            //only cache enough Segments to merge.
    //            if (count == 0)
    //              segmentsToMerge += ((0, segment))
    //            else
    //              segmentsToMerge += ((count, segment))
    //          }
    //
    //          segmentsToMerge.size >= take
    //      }
    //
    //    //Important! returned segments should always be in order
    //    segmentsToMerge
    //      .sortBy(_._1)
    //      .take(take)
    //      .map(_._2)
    //      .sortBy(_.minKey)
    ???
  }
}

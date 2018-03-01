/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import swaydb.core.data.{KeyValueWriteOnly, Value}
import swaydb.core.map.Map
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable

private[core] object SegmentAssigner {

  def assign(map: Map[Slice[Byte], Value],
             targetSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assign(Segment.tempMinMaxKeyValues(map), targetSegments).keys

  def assign(inputSegments: Iterable[Segment],
             targetSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assign(Segment.tempMinMaxKeyValues(inputSegments), targetSegments).keys

  def assign(keyValues: Slice[KeyValueWriteOnly],
             segments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): mutable.Map[Segment, Slice[KeyValueWriteOnly]] = {
    import ordering._
    val assignmentsMap = mutable.Map.empty[Segment, Slice[KeyValueWriteOnly]]
    val segmentsIterator = segments.iterator

    def getNextSegmentMayBe() = if (segmentsIterator.hasNext) Some(segmentsIterator.next()) else None

    def assignKeyValueToSegment(segment: Segment, keyValue: Slice[KeyValueWriteOnly]): Unit =
      assignmentsMap.get(segment) match {
        case Some(currentSlice) =>
          assignmentsMap += (segment -> keyValues.slice(currentSlice.fromOffset, currentSlice.toOffset + 1))
        case None =>
          assignmentsMap += (segment -> keyValue)
      }

    @tailrec
    def assign(remainingKeyValues: Slice[KeyValueWriteOnly],
               thisSegmentMayBe: Option[Segment],
               nextSegmentMayBe: Option[Segment]): Unit = {
      (remainingKeyValues.headOption, thisSegmentMayBe, nextSegmentMayBe) match {
        //add this map if this is the new smallest key or if this key belong to this map or if there is no next map
        case (Some(keyValue), Some(thisSegment), _) if keyValue.key <= thisSegment.minKey || Segment.belongsTo(keyValue, thisSegment) || nextSegmentMayBe.isEmpty =>
          assignKeyValueToSegment(thisSegment, remainingKeyValues.headSlice)
          assign(remainingKeyValues.drop(1), thisSegmentMayBe, nextSegmentMayBe)

        // is this a gap key between thisSegment and the nextSegment
        case (Some(keyValue), Some(thisSegment), Some(nextSegment)) if keyValue.key < nextSegment.minKey =>
          //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
          if (assignmentsMap.contains(thisSegment)) {
            assignKeyValueToSegment(thisSegment, remainingKeyValues.headSlice)
            assign(remainingKeyValues.drop(1), thisSegmentMayBe, nextSegmentMayBe)
          } else
            assign(remainingKeyValues, Some(nextSegment), getNextSegmentMayBe())

        // jump to next map
        case (Some(_), Some(_), Some(nextSegment)) =>
          assign(remainingKeyValues, Some(nextSegment), getNextSegmentMayBe())

        case _ =>
      }
    }

    if (segments.size == 1) {
      assignKeyValueToSegment(segmentsIterator.next(), keyValues)
      assignmentsMap
    } else if (segmentsIterator.hasNext) {
      assign(keyValues, Some(segmentsIterator.next()), getNextSegmentMayBe())
      assignmentsMap
    } else {
      mutable.Map.empty
    }
  }

}

/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.segment

import swaydb.core.data.{KeyValue, Memory, MemoryOptional, Value}
import swaydb.core.map.Map
import swaydb.core.segment.format.a.block.SegmentIO
import swaydb.core.segment.merge.MergeList
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOptional}

import scala.annotation.tailrec
import scala.collection.mutable

private[core] object SegmentAssigner {

  def assignMinMaxOnlyUnsafe(inputSegments: Iterable[Segment],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafe(2, Segment.tempMinMaxKeyValues(inputSegments), targetSegments).keys

  def assignMinMaxOnlyUnsafe(map: Map[SliceOptional[Byte], MemoryOptional, Slice[Byte], Memory],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                segmentIO: SegmentIO): Iterable[Segment] =
    SegmentAssigner.assignUnsafe(2, Segment.tempMinMaxKeyValues(map), targetSegments).keys

  def assignUnsafe(keyValues: Slice[KeyValue],
                   segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): mutable.Map[Segment, Slice[KeyValue]] =
    assignUnsafe(
      keyValuesCount = keyValues.size,
      keyValues = keyValues,
      segments = segments
    )

  def assignUnsafe(keyValuesCount: Int,
                   keyValues: Iterable[KeyValue],
                   segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): mutable.Map[Segment, Slice[KeyValue]] = {
    import keyOrder._
    val assignmentsMap = mutable.Map.empty[Segment, Slice[KeyValue]]
    val segmentsIterator = segments.iterator

    def getNextSegmentMayBe() = if (segmentsIterator.hasNext) segmentsIterator.next() else Segment.Null

    def assignKeyValueToSegment(segment: Segment,
                                keyValue: KeyValue,
                                remainingKeyValues: Int): Unit =
      assignmentsMap.get(segment) match {
        case Some(currentAssignments) =>
          try
            currentAssignments add keyValue
          catch {
            /**
             * ArrayIndexOutOfBoundsException can occur when the size of an unopened Group's key-value was not accounted
             * for at the time of Slice initialisation.
             *
             * This failure is not expected to occur often and it will be more efficient to extend the exiting assignments Slice
             * whenever required.
             */
            case _: ArrayIndexOutOfBoundsException =>
              val initial = Slice.create[KeyValue](currentAssignments.size + remainingKeyValues + 1)
              initial addAll currentAssignments
              initial add keyValue
              assignmentsMap += (segment -> initial)
          }

        case None =>
          //+1 for cases when a Range might extend to the next Segment.
          val initial = Slice.create[KeyValue](remainingKeyValues + 1)
          initial add keyValue
          assignmentsMap += (segment -> initial)
      }

    @tailrec
    def assign(remainingKeyValues: MergeList[Memory.Range, KeyValue],
               thisSegmentMayBe: SegmentOptional,
               nextSegmentMayBe: SegmentOptional): Unit =
      (remainingKeyValues.headOrNull, thisSegmentMayBe, nextSegmentMayBe) match {
        //add this key-value if it is the new smallest key or if this key belong to this Segment or if there is no next Segment
        case (keyValue: KeyValue, thisSegment: Segment, _) if keyValue.key <= thisSegment.minKey || Segment.belongsTo(keyValue, thisSegment) || nextSegmentMayBe.isNoneSON =>
          keyValue match {
            case keyValue: KeyValue.Fixed =>
              assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
              assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

            case keyValue: KeyValue.Range =>
              nextSegmentMayBe match {
                case nextSegment: Segment if keyValue.toKey > nextSegment.minKey =>
                  val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                  val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                  val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.None, rangeValue = rangeValue)

                  assignKeyValueToSegment(thisSegment, thisSegmentsRange, remainingKeyValues.size)
                  assign(remainingKeyValues.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())

                case _ =>
                  assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                  assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
              }
          }


        // is this a gap key between thisSegment and the nextSegment
        case (keyValue: KeyValue, thisSegment: Segment, nextSegment: Segment) if keyValue.key < nextSegment.minKey =>
          keyValue match {
            case keyValue: KeyValue.Fixed =>
              //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
              if (assignmentsMap.contains(thisSegment)) {
                assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
              } else {
                assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
              }

            case keyValue: KeyValue.Range =>
              nextSegmentMayBe match {
                case nextSegment: Segment if keyValue.toKey > nextSegment.minKey =>
                  //if it's a gap Range key-value and it's flows onto the next Segment, just jump to the next Segment.
                  assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())

                case _ =>
                  //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
                  //same code as above, need to push it to a common function.
                  if (assignmentsMap.contains(thisSegment)) {
                    assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                    assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                  } else {
                    assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
                  }
              }
          }

        case (_: KeyValue, _: Segment, nextSegment: Segment) =>
          assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())

        case (_, _, _) =>
          ()
      }

    if (Segment hasOnlyOneSegment segments) //.size iterates the entire Iterable which is not needed.
      mutable.Map((segments.head, Slice.from(keyValues, keyValuesCount)))
    else if (segmentsIterator.hasNext) {
      assign(MergeList(keyValuesCount, keyValues.iterator), segmentsIterator.next(), getNextSegmentMayBe())
      assignmentsMap map {
        case (segment, keyValues) =>
          (segment, keyValues.close())
      }
    }
    else
      assignmentsMap
  }
}

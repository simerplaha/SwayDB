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

import swaydb.core.data.{KeyValue, Transient, Value}
import swaydb.core.map.Map
import swaydb.core.map.serializer.RangeValueSerializers._
import swaydb.core.util.TryUtil
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

private[core] object SegmentAssigner {

  def assign(map: Map[Slice[Byte], Value],
             targetSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assign(Segment.tempMinMaxKeyValues(map), targetSegments).get.keys

  def assign(inputSegments: Iterable[Segment],
             targetSegments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assign(Segment.tempMinMaxKeyValues(inputSegments), targetSegments).get.keys

  def assign(keyValues: Slice[KeyValue.WriteOnly],
             segments: Iterable[Segment])(implicit ordering: Ordering[Slice[Byte]]): Try[mutable.Map[Segment, Slice[KeyValue.WriteOnly]]] = {
    import ordering._
    val assignmentsMap = mutable.Map.empty[Segment, Slice[KeyValue.WriteOnly]]
    val segmentsIterator = segments.iterator

    def getNextSegmentMayBe() = if (segmentsIterator.hasNext) Some(segmentsIterator.next()) else None

    def assignKeyValueToSegment(segment: Segment,
                                keyValue: KeyValue.WriteOnly,
                                remainingKeyValues: Int): Unit =
      assignmentsMap.get(segment) match {
        case Some(currentAssignments) =>
          assignmentsMap += (segment -> (currentAssignments add keyValue))
        case None =>
          //+1 for cases when a Range might extend to the next Segment.
          val initial = Slice.create[KeyValue.WriteOnly](remainingKeyValues + 1)
          initial add keyValue
          assignmentsMap += (segment -> initial)
      }

    var stashKeyValue = Option.empty[KeyValue.RangeWriteOnly]

    @tailrec
    def assign(remainingKeyValues: Slice[KeyValue.WriteOnly],
               thisSegmentMayBe: Option[Segment],
               nextSegmentMayBe: Option[Segment]): Try[Unit] = {

      def dropKeyValue(stash: Option[KeyValue.RangeWriteOnly] = None) =
        if (stashKeyValue.isDefined) {
          stashKeyValue = stash
          remainingKeyValues
        } else {
          stashKeyValue = stash
          remainingKeyValues.drop(1)
        }

      (stashKeyValue orElse remainingKeyValues.headOption, thisSegmentMayBe, nextSegmentMayBe) match {
        //add this key-value if it is the new smallest key or if this key belong to this Segment or if there is no next Segment
        case (Some(keyValue: KeyValue), Some(thisSegment), _) if keyValue.key <= thisSegment.minKey || Segment.belongsTo(keyValue, thisSegment) || nextSegmentMayBe.isEmpty =>
          keyValue match {
            case keyValue: KeyValue.FixedWriteOnly =>
              assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
              assign(dropKeyValue(), thisSegmentMayBe, nextSegmentMayBe)

            case keyValue: KeyValue.RangeWriteOnly =>
              nextSegmentMayBe match {
                case Some(nextSegment) if keyValue.toKey > nextSegment.minKey =>
                  keyValue.fetchFromAndRangeValue match {
                    case Success((fromValue, rangeValue)) =>
                      val thisSegmentsRange = Transient.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue, falsePositiveRate = 1, None)
                      val nextSegmentsRange = Transient.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = None, rangeValue = rangeValue, falsePositiveRate = 1, None)
                      assignKeyValueToSegment(thisSegment, thisSegmentsRange, remainingKeyValues.size)
                      assign(dropKeyValue(Some(nextSegmentsRange)), Some(nextSegment), getNextSegmentMayBe())

                    case Failure(exception) =>
                      Failure(exception)
                  }

                case _ =>
                  assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                  assign(dropKeyValue(), thisSegmentMayBe, nextSegmentMayBe)
              }
          }


        // is this a gap key between thisSegment and the nextSegment
        case (Some(keyValue: KeyValue), Some(thisSegment), Some(nextSegment)) if keyValue.key < nextSegment.minKey =>
          keyValue match {
            case keyValue: KeyValue.FixedWriteOnly =>
              //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
              if (assignmentsMap.contains(thisSegment)) {
                assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                assign(dropKeyValue(), thisSegmentMayBe, nextSegmentMayBe)
              } else
                assign(remainingKeyValues, Some(nextSegment), getNextSegmentMayBe())

            case keyValue: KeyValue.RangeWriteOnly =>
              nextSegmentMayBe match {
                case Some(nextSegment) if keyValue.toKey > nextSegment.minKey =>
                  //if it's a gap Range key-value and it's flows onto the next Segment, just jump to the next Segment.
                  assign(dropKeyValue(Some(keyValue)), Some(nextSegment), getNextSegmentMayBe())

                case _ =>
                  if (assignmentsMap.contains(thisSegment)) {
                    assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                    assign(dropKeyValue(), thisSegmentMayBe, nextSegmentMayBe)
                  } else
                    assign(remainingKeyValues, Some(nextSegment), getNextSegmentMayBe())
              }
          }

        case (Some(_), Some(_), Some(nextSegment)) =>
          assign(remainingKeyValues, Some(nextSegment), getNextSegmentMayBe())

        case (_, _, _) =>
          TryUtil.successUnit
      }
    }

    if (segments.size == 1)
      Success(mutable.Map((segments.head, keyValues)))
    else if (segmentsIterator.hasNext)
      assign(keyValues, Some(segmentsIterator.next()), getNextSegmentMayBe()) map {
        _ =>
          assignmentsMap map {
            case (segment, keyValues) =>
              (segment, keyValues.close())
          }
      }
    else
      Success(assignmentsMap)
  }
}
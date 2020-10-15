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

package swaydb.core.segment

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory, MemoryOption, Value}
import swaydb.core.util.DropIterator
import swaydb.core.util.skiplist.SkipList
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[core] sealed trait AssignmentOption[+A]

private[core] sealed trait Assignment[+A] extends AssignmentOption[A] {
  def add(keyValue: KeyValue): Unit

  def isGap: Boolean

  def isAssigned: Boolean =
    !isGap

  def existAssigned(assignment: Assignment.Assigned => Boolean): Boolean

  def segment: SegmentOption
}

private[core] case object Assignment {

  case object Null extends AssignmentOption[Nothing]

  case class Assigned(segment: Segment, keyValues: Slice[KeyValue]) extends Assignment[Nothing] {
    override def add(keyValue: KeyValue): Unit =
      keyValues add keyValue

    override def isGap: Boolean =
      false

    override def existAssigned(assignment: Assigned => Boolean): Boolean =
      assignment(this)

    def toTuple(): (Segment, Slice[KeyValue]) =
      (segment, keyValues)
  }

  case class Gap[A](aggregator: Aggregator[KeyValue, A]) extends Assignment[A] {
    override def add(keyValue: KeyValue): Unit =
      aggregator add keyValue

    override def isGap: Boolean =
      true

    override def existAssigned(assignment: Assigned => Boolean): Boolean =
      false

    override def segment: SegmentOption = Segment.Null
  }
}

private[core] object SegmentAssigner {

  def assignMinMaxOnlyUnsafe(inputSegments: Iterable[Segment],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafe(2 * inputSegments.size, Segment.tempMinMaxKeyValues(inputSegments), targetSegments).keys

  def assignMinMaxOnlyUnsafe(keyValues: Either[SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], Slice[Memory]],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    keyValues match {
      case Left(value) =>
        assignMinMaxOnlyUnsafe(value, targetSegments)

      case Right(value) =>
        assignMinMaxOnlyUnsafe(value, targetSegments)
    }

  def assignMinMaxOnlyUnsafe(input: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafe(2, Segment.tempMinMaxKeyValues(input), targetSegments).keys

  def assignMinMaxOnlyUnsafe(input: Slice[Memory],
                             targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafe(2, Segment.tempMinMaxKeyValues(input), targetSegments).keys

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
    val buffer =
      assignUnsafe(
        keyValuesCount = keyValuesCount,
        keyValues = keyValues,
        segments = segments,
        noGaps = true
      )(keyOrder, Aggregator.Creator.nothing()).asInstanceOf[ListBuffer[Assignment.Assigned]]

    val map = mutable.Map.empty[Segment, Slice[KeyValue]]

    buffer foreach {
      assigned =>
        map.put(assigned.segment, assigned.keyValues)
    }

    map
  }

  def assignUnsafeWithGaps[A](keyValuesCount: Int,
                              keyValues: Iterable[KeyValue],
                              segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                           gapCreator: Aggregator.CreatorSizeable[KeyValue, A]): ListBuffer[Assignment[A]] =
    assignUnsafe(
      keyValuesCount = keyValuesCount,
      keyValues = keyValues,
      segments = segments,
      noGaps = false
    )

  /**
   * @param keyValuesCount keyValuesCount is needed here because keyValues could be a [[ConcurrentSkipList]]
   *                       where calculating size is not constant time.
   */
  private def assignUnsafe[A](keyValuesCount: Int,
                              keyValues: Iterable[KeyValue],
                              segments: Iterable[Segment],
                              noGaps: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               gapCreator: Aggregator.CreatorSizeable[KeyValue, A]): ListBuffer[Assignment[A]] = {
    if (noGaps && Segment.hasOnlyOneSegment(segments)) { //.size iterates the entire Iterable which is not needed.
      val assignments = ListBuffer[Assignment[A]]()
      assignments += Assignment.Assigned(segments.head, Slice.from(keyValues, keyValuesCount))
      assignments
    } else {
      import keyOrder._

      val assignments = ListBuffer.empty[Assignment[A]]

      val segmentsIterator = segments.iterator

      def getNextSegmentMayBe() = if (segmentsIterator.hasNext) segmentsIterator.next() else Segment.Null

      def assignKeyValueToSegment(segmentToAdd: Segment,
                                  keyValue: KeyValue,
                                  remainingKeyValues: Int): Unit =
        assignments.lastOption match {
          case Some(Assignment.Assigned(lastSegment, keyValues)) if lastSegment.segmentId == segmentToAdd.segmentId =>
            keyValues add keyValue

          case _ =>
            //+1 for cases when a Range might extend to the next Segment.
            val keyValues = Slice.of[KeyValue](remainingKeyValues + 1) add keyValue
            val assignment = Assignment.Assigned(segmentToAdd, keyValues)
            assignments += assignment
        }

      def assignKeyValueToGap(keyValue: KeyValue,
                              remainingKeyValues: Int): Unit =
        assignments.lastOption match {
          case Some(Assignment.Gap(aggregator)) =>
            aggregator add keyValue

          case _ =>
            //ignore assigned a create a new gap
            val gapAggregator = gapCreator.createNewSizeHint(remainingKeyValues + 1)
            gapAggregator add keyValue
            assignments += Assignment.Gap(gapAggregator)
        }

      @tailrec
      def assign(remainingKeyValues: DropIterator[Memory.Range, KeyValue],
                 thisSegmentMayBe: SegmentOption,
                 nextSegmentMayBe: SegmentOption): Unit = {
        val keyValue = remainingKeyValues.headOrNull

        if (keyValue != null)
          thisSegmentMayBe match {
            case Segment.Null =>
              if (noGaps)
                throw new Exception("Cannot assign key-value to Null Segment.")
              else
                remainingKeyValues.iterator foreach {
                  keyValue =>
                    assignKeyValueToGap(keyValue, remainingKeyValues.size)
                }

            case thisSegment: Segment =>
              val keyCompare = keyOrder.compare(keyValue.key, thisSegment.minKey)

              //0 = Unknown. 1 = true, -1 = false
              var _belongsTo = 0

              def getKeyBelongsTo(): Boolean = {
                if (_belongsTo == 0)
                  if (Segment.belongsTo(keyValue, thisSegment))
                    _belongsTo = 1
                  else
                    _belongsTo = -1

                _belongsTo == 1
              }

              //add this key-value if it is the new smallest key or if this key belong to this Segment or if there is no next Segment
              if (keyCompare <= 0 || getKeyBelongsTo() || nextSegmentMayBe.isNoneS)
                keyValue match {
                  case keyValue: KeyValue.Fixed =>
                    if (noGaps || keyCompare == 0 || getKeyBelongsTo()) //if this key-value should be added to thisSegment
                      assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                    else //gap key
                      assignKeyValueToGap(keyValue, remainingKeyValues.size)

                    assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                  case keyValue: KeyValue.Range =>
                    nextSegmentMayBe match {
                      //check if this range key-value spreads onto the next segment
                      case nextSegment: Segment if keyValue.toKey > nextSegment.minKey =>
                        val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                        val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                        val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.Null, rangeValue = rangeValue)

                        if (noGaps || keyCompare == 0 || getKeyBelongsTo()) //should add to this segment
                          assignKeyValueToSegment(thisSegment, thisSegmentsRange, remainingKeyValues.size)
                        else //should add as a gap
                          assignKeyValueToGap(thisSegmentsRange, remainingKeyValues.size)

                        assign(remainingKeyValues.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())

                      case _ =>
                        //belongs to current segment
                        if (noGaps || keyCompare == 0 || getKeyBelongsTo())
                          assignKeyValueToSegment(thisSegment, keyValue, remainingKeyValues.size)
                        else
                          assignKeyValueToGap(keyValue, remainingKeyValues.size)

                        assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                    }
                }
              else
                nextSegmentMayBe match {
                  case Segment.Null =>
                    if (noGaps)
                      throw new Exception("Cannot assign key-value to Null next Segment.")
                    else
                      remainingKeyValues.iterator foreach {
                        keyValue =>
                          assignKeyValueToGap(keyValue, remainingKeyValues.size)
                      }

                  case nextSegment: Segment =>
                    if (keyValue.key < nextSegment.minKey) // is this a gap key between thisSegment and the nextSegment
                      keyValue match {
                        case _: KeyValue.Fixed =>
                          if (noGaps) {
                            //check if a key-value is already assigned to thisSegment. Else if thisSegment is empty jump to next
                            //there is no point adding a single key-value to a Segment.
                            assignments.lastOption match {
                              case Some(Assignment.Assigned(segment, keyValues)) if segment.segmentId == thisSegment.segmentId =>
                                keyValues add keyValue
                                assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                              case _ =>
                                assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
                            }
                          } else {
                            //Is a gap key
                            assignKeyValueToGap(keyValue, remainingKeyValues.size)
                            assign(remainingKeyValues.dropHead(), nextSegment, getNextSegmentMayBe())
                          }

                        case keyValue: KeyValue.Range =>
                          nextSegmentMayBe match {
                            case nextSegment: Segment if keyValue.toKey > nextSegment.minKey =>
                              //if it's a gap Range key-value and it's flows onto the next Segment.
                              if (noGaps) {
                                //no gaps allowed, jump to next segment and avoid splitting the range.
                                assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
                              } else {
                                //perform a split
                                val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                                val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                                val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.Null, rangeValue = rangeValue)
                                assignKeyValueToGap(thisSegmentsRange, remainingKeyValues.size)

                                assign(remainingKeyValues.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())
                              }

                            case _ =>
                              //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
                              //same code as above, need to push it to a common function.
                              if (noGaps) {
                                assignments.lastOption match {
                                  case Some(Assignment.Assigned(segment, keyValues)) if segment.segmentId == thisSegment.segmentId =>
                                    keyValues add keyValue
                                    assign(remainingKeyValues.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                                  case _ =>
                                    assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
                                }
                              } else {
                                assignKeyValueToGap(keyValue, remainingKeyValues.size)
                                assign(remainingKeyValues.dropHead(), nextSegment, getNextSegmentMayBe())
                              }
                          }
                      }
                    else //jump to next Segment.
                      assign(remainingKeyValues, nextSegment, getNextSegmentMayBe())
                }
          }
      }

      if (segmentsIterator.hasNext)
        assign(DropIterator(keyValuesCount, keyValues.iterator), segmentsIterator.next(), getNextSegmentMayBe())

      assignments
    }
  }
}

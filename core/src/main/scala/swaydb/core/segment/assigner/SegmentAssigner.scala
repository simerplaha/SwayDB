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

package swaydb.core.segment.assigner

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory, MemoryOption, Value}
import swaydb.core.segment.{PersistentSegmentMany, Segment}
import swaydb.core.segment.assigner.AssignmentTarget._
import swaydb.core.segment.ref.SegmentRef
import swaydb.core.util.DropIterator
import swaydb.core.util.skiplist.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
 * Assigns [[Assignable]] types to Segments.
 */

private[core] object SegmentAssigner {

  def assignMinMaxOnlyUnsafeNoGaps(inputSegments: Iterable[Assignable.Collection],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(Segment.tempMinMaxKeyValues(inputSegments), targetSegments).map(_.segment)

  def assignMinMaxOnlyUnsafeNoGaps(input: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(Segment.tempMinMaxKeyValues(input), targetSegments).map(_.segment)

  def assignMinMaxOnlyUnsafeNoGaps(input: Slice[Memory],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(Segment.tempMinMaxKeyValues(input), targetSegments).map(_.segment)

  def assignUnsafeNoGaps(keyValues: Slice[Assignable],
                         segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): ListBuffer[SegmentAssignment[Nothing, ListBuffer[Assignable], Segment]] =
    assignUnsafe[Nothing, ListBuffer[Assignable], Segment](
      keyValues = keyValues.iterator,
      segmentsIterator = segments.iterator,
      noGaps = true
    )

  def assignUnsafeNoGaps(keyValues: Iterable[Assignable],
                         segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): ListBuffer[SegmentAssignment[Nothing, ListBuffer[Assignable], Segment]] =
    assignUnsafe[Nothing, ListBuffer[Assignable], Segment](
      keyValues = keyValues.iterator,
      segmentsIterator = segments.iterator,
      noGaps = true
    )

  def assignUnsafeGaps[GAP](keyValues: Slice[Assignable],
                            segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[SegmentAssignment[GAP, ListBuffer[Assignable], Segment]] =
    assignUnsafe[GAP, ListBuffer[Assignable], Segment](
      keyValues = keyValues.iterator,
      segmentsIterator = segments.iterator,
      noGaps = false
    )

  def assignUnsafeGaps[GAP](keyValues: Iterable[Assignable],
                            segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[SegmentAssignment[GAP, ListBuffer[Assignable], Segment]] =
    assignUnsafe[GAP, ListBuffer[Assignable], Segment](
      keyValues = keyValues.iterator,
      segmentsIterator = segments.iterator,
      noGaps = false
    )

  def assignUnsafeGapsSegmentRef[GAP](keyValues: Iterator[Assignable],
                                      segments: Iterable[SegmentRef])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[SegmentAssignment[GAP, ListBuffer[Assignable], SegmentRef]] =
    assignUnsafe[GAP, ListBuffer[Assignable], SegmentRef](
      keyValues = keyValues,
      segmentsIterator = segments.iterator,
      noGaps = false
    )

  def assignUnsafeGapsSegmentRef[GAP](keyValues: Iterator[Assignable],
                                      segments: Iterator[SegmentRef])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                      gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[SegmentAssignment[GAP, ListBuffer[Assignable], SegmentRef]] =
    assignUnsafe[GAP, ListBuffer[Assignable], SegmentRef](
      keyValues = keyValues,
      segmentsIterator = segments,
      noGaps = false
    )

  def assignUnsafeGaps[GAP, MID <: Iterable[_], SEG >: Null](keyValues: Iterator[Assignable],
                                                             segments: Iterator[SEG])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                      midCreator: Aggregator.Creator[Assignable, MID],
                                                                                      gapCreator: Aggregator.Creator[Assignable, GAP],
                                                                                      assignmentTarget: AssignmentTarget[SEG]): ListBuffer[SegmentAssignment[GAP, MID, SEG]] =
    assignUnsafe[GAP, MID, SEG](
      keyValues = keyValues,
      segmentsIterator = segments,
      noGaps = false
    )

  /**
   * Assigns [[Assignable]] to target Segments.
   *
   * It avoids Segment expansion as much as possible and assign collections
   * of key-values [[Assignable.Collection]] directly to a Segment so that
   * the Segment can decide if expansion is required. Assigned Segments that
   * are not expanded can be directly transferred onto a new Segments during
   * compaction without reading them into JVM heap.
   */
  private def assignUnsafe[GAP, MID <: Iterable[_], SEG >: Null](keyValues: Iterator[Assignable],
                                                                 segmentsIterator: Iterator[SEG],
                                                                 noGaps: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                  gapCreator: Aggregator.Creator[Assignable, GAP],
                                                                                  midCreator: Aggregator.Creator[Assignable, MID],
                                                                                  assignmentTarget: AssignmentTarget[SEG]): ListBuffer[SegmentAssignment[GAP, MID, SEG]] = {
    import keyOrder._

    val assignments = ListBuffer.empty[SegmentAssignment[GAP, MID, SEG]]

    @inline def getNextSegmentMayBe(): SEG = if (segmentsIterator.hasNext) segmentsIterator.next() else null

    /**
     * Checks if this Segment has non-gap key-values assigned.
     */
    @inline def segmentHasNonGapedKeyValuesAssigned(segment: SEG): Boolean =
      assignments.lastOption match {
        case Some(assignment) =>
          assignment.segment == segment &&
            assignment.midOverlap.result.lastOption.exists(_.isInstanceOf[KeyValue])

        case None =>
          false
      }

    def assignToSegment(assignable: Assignable,
                        assignTo: SEG): Unit =
      assignments.lastOption match {
        case Some(SegmentAssignment(bufferSegment, _, mid, _)) if bufferSegment == assignTo =>
          mid add assignable

        case _ =>
          assignments +=
            SegmentAssignment(
              segment = assignTo,
              //if noGaps is true then gapCreator will return Nothing Aggregator which cannot be accessed.
              //So use null so that no objects are created.
              headGap = if (noGaps) null else gapCreator.createNew(),
              midOverlap = midCreator.createNew(assignable),
              tailGap = if (noGaps) null else gapCreator.createNew(),
            )
      }

    def assignToGap(assignable: Assignable,
                    assignTo: SEG): Unit =
      assignments.lastOption match {
        case Some(SegmentAssignment(bufferSegment, headGap, midOverlap, tailGap)) if bufferSegment == assignTo =>
          if (midOverlap.result.isEmpty && assignable.key < assignTo.minKey)
            headGap add assignable
          else
            tailGap add assignable

        case _ =>
          //assign the key to headGap or tailGap depending on it's position.
          val (headGap, tailGap) =
            if (assignable.key < assignTo.minKey) {
              val headGap = gapCreator.createNew()
              headGap add assignable
              (headGap, gapCreator.createNew())
            } else {
              val tailGap = gapCreator.createNew()
              tailGap add assignable
              (gapCreator.createNew(), tailGap)
            }

          assignments +=
            SegmentAssignment(
              segment = assignTo,
              headGap = headGap,
              midOverlap = midCreator.createNew(),
              tailGap = tailGap
            )
      }

    @tailrec
    def assign(remaining: DropIterator[Memory.Range, Assignable],
               thisSegmentMayBe: SEG,
               nextSegmentMayBe: SEG): Unit = {
      val assignable = remaining.headOrNull

      if (assignable != null)
        thisSegmentMayBe match {
          case null =>
            //this would should never occur. If Segments were empty then the key-values should be copied.
            throw new Exception("Cannot assign key-value to Null Segment.")

          case thisSegment: SEG =>
            val thisSegmentMinKeyCompare = keyOrder.compare(assignable.key, thisSegment.minKey)

            //0 = Unknown. 1 = true, -1 = false
            var _belongsTo = 0

            @inline def keyBelongsToThisSegmentNoSpread(): Boolean = {
              //avoid computing multiple times.
              if (_belongsTo == 0)
                if (Segment.overlaps(assignable, thisSegment.minKey, thisSegment.maxKey))
                  _belongsTo = 1
                else
                  _belongsTo = -1

              _belongsTo == 1
            }

            @inline def spreadToNextSegment(collection: Assignable.Collection, segment: SEG): Boolean =
              collection.maxKey match {
                case MaxKey.Fixed(maxKey) =>
                  maxKey >= segment.minKey

                case MaxKey.Range(_, maxKey) =>
                  maxKey > segment.minKey
              }

            /**
             * Handle if this key-value if it is the new smallest key or if this key belong to this Segment or if there is no next Segment
             */
            if (thisSegmentMinKeyCompare <= 0 || nextSegmentMayBe == null || keyBelongsToThisSegmentNoSpread())
              assignable match {
                case assignable: Assignable.Collection =>
                  nextSegmentMayBe match {
                    case nextSegment: SEG if spreadToNextSegment(assignable, nextSegment) => //if Segment spreads onto next Segment
                      assignable match {
                        case many: PersistentSegmentMany => //always expand to cover cases when last SegmentRef is a gap Segment.
                          val segmentIterator = DropIterator[Memory.Range, Assignable](many.segmentRefs())

                          val newRemaining = segmentIterator append remaining.dropHead()

                          assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)

                        case _ =>
                          val segmentIterator = DropIterator[Memory.Range, Assignable](assignable.iterator())

                          val newRemaining = segmentIterator append remaining.dropHead()

                          assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)
                      }

                    case _ =>
                      if (noGaps || thisSegmentMinKeyCompare == 0 || keyBelongsToThisSegmentNoSpread()) { //if this Segment should be added to thisSegment
                        assignable match {
                          case many: PersistentSegmentMany if !noGaps => //always expand to cover cases when last SegmentRef is a gap Segment.
                            val segmentIterator = DropIterator[Memory.Range, Assignable](many.segmentRefs())

                            val newRemaining = segmentIterator append remaining.dropHead()

                            assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)

                          case _ =>
                            assignToSegment(assignable = assignable, assignTo = thisSegment)
                            assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                        }
                      } else { //gap Segment
                        assignToGap(assignable = assignable, assignTo = thisSegment)
                        assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                      }
                  }

                case keyValue: KeyValue.Fixed =>
                  if (noGaps || thisSegmentMinKeyCompare == 0 || keyBelongsToThisSegmentNoSpread()) //if this key-value should be added to thisSegment
                    assignToSegment(assignable = keyValue, assignTo = thisSegment)
                  else //gap key
                    assignToGap(assignable = keyValue, assignTo = thisSegment)

                  assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                case keyValue: KeyValue.Range =>
                  nextSegmentMayBe match {
                    //check if this range key-value spreads onto the next segment
                    case nextSegment: SEG if keyValue.toKey > nextSegment.minKey =>
                      val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                      val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                      val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.Null, rangeValue = rangeValue)

                      if (noGaps || thisSegmentMinKeyCompare == 0 || keyBelongsToThisSegmentNoSpread()) //should add to this segment
                        assignToSegment(assignable = thisSegmentsRange, assignTo = thisSegment)
                      else //should add as a gap
                        assignToGap(assignable = thisSegmentsRange, assignTo = thisSegment)

                      assign(remaining.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())

                    case _ =>
                      //belongs to current segment
                      if (noGaps || thisSegmentMinKeyCompare == 0 || keyBelongsToThisSegmentNoSpread())
                        assignToSegment(assignable = keyValue, assignTo = thisSegment)
                      else
                        assignToGap(assignable = keyValue, assignTo = thisSegment)

                      assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                  }
              }
            else

            /**
             * Handle gap key between [[thisSegment]] and [[nextSegmentMayBe]]
             */
              nextSegmentMayBe match {
                case null =>
                  if (noGaps)
                    throw new Exception("Cannot assign key-value to Null next Segment.")
                  else
                    remaining.iterator foreach {
                      keyValue =>
                        assignToGap(assignable = keyValue, assignTo = thisSegment)
                    }

                case nextSegment: SEG =>
                  if (assignable.key < nextSegment.minKey) // is this a gap key between thisSegment and the nextSegment
                    assignable match {
                      case assignable: Assignable.Collection =>
                        if (spreadToNextSegment(assignable, nextSegment)) {
                          //No need to expand the Segment if thisSegment does not have key-values assigned.
                          //Simply jump to the next Segment for a higher chance of the Segment getting assigned without expanding.
                          if (!segmentHasNonGapedKeyValuesAssigned(thisSegment))
                            assign(remaining, nextSegmentMayBe, getNextSegmentMayBe())
                          else
                            assignable match {
                              case many: PersistentSegmentMany => //always expand to cover cases when last SegmentRef is a gap Segment.
                                val segmentIterator = DropIterator[Memory.Range, Assignable](many.segmentRefs())

                                val newRemaining = segmentIterator append remaining.dropHead()

                                assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)

                              case _ =>
                                //if this Segment spreads onto next Segment read all key-values and assign.
                                val segmentIterator = DropIterator[Memory.Range, Assignable](assignable.iterator())

                                val newRemaining = segmentIterator append remaining.dropHead()

                                assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)
                            }
                        } else {
                          //does not spread onto next Segment.
                          if (noGaps) {
                            if (thisSegmentMinKeyCompare == 0 || keyBelongsToThisSegmentNoSpread()) { //if this Segment should be added to thisSegment
                              assignToSegment(assignable = assignable, assignTo = thisSegment)
                              assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                            } else { //so this is a gap Segment which can also be assigned to nextSegment. Always defer assignment.
                              assign(remaining, nextSegment, getNextSegmentMayBe())
                            }
                          } else {
                            //gap segment
                            assignToGap(assignable, thisSegment)
                            assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                          }
                        }

                      case _: KeyValue.Fixed =>
                        if (noGaps) {
                          //check if a key-value is already assigned to thisSegment. Else if thisSegment is empty jump to next
                          //there is no point adding a single key-value to a Segment.
                          assignments.lastOption match {
                            case Some(SegmentAssignment(segment, _, keyValues, _)) if segment == thisSegment =>
                              keyValues add assignable
                              assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                            case _ =>
                              assign(remaining, nextSegment, getNextSegmentMayBe())
                          }
                        } else {
                          //Is a gap key
                          assignToGap(assignable, thisSegment)
                          assign(remaining.dropHead(), nextSegment, getNextSegmentMayBe())
                        }

                      case keyValue: KeyValue.Range =>
                        if (keyValue.toKey > nextSegment.minKey) {
                          //if it's a gap Range key-value and it's flows onto the next Segment.
                          if (noGaps) {
                            //no gaps allowed, jump to next segment and avoid splitting the range.
                            assign(remaining, nextSegment, getNextSegmentMayBe())
                          } else {
                            //perform a split
                            val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                            val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                            val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.Null, rangeValue = rangeValue)
                            assignToGap(thisSegmentsRange, thisSegment)

                            assign(remaining.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())
                          }
                        } else {
                          //ignore if a key-value is not already assigned to thisSegment. No point adding a single key-value to a Segment.
                          //same code as above, need to push it to a common function.
                          if (noGaps) {
                            assignments.lastOption match {
                              case Some(SegmentAssignment(segment, _, keyValues, _)) if segment == thisSegment =>
                                keyValues add keyValue
                                assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                              case _ =>
                                assign(remaining, nextSegment, getNextSegmentMayBe())
                            }
                          } else {
                            assignToGap(keyValue, thisSegment)
                            assign(remaining.dropHead(), nextSegment, getNextSegmentMayBe())
                          }
                        }
                    }
                  else //jump to next Segment.
                    assign(remaining, nextSegment, getNextSegmentMayBe())
              }
        }
    }

    if (segmentsIterator.hasNext)
      assign(DropIterator(keyValues), segmentsIterator.next(), getNextSegmentMayBe())

    assignments
  }
}

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

package swaydb.core.segment.assigner

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory, MemoryOption, Value}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.DropIterator
import swaydb.core.util.skiplist.SkipList
import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object SegmentAssigner {

  def assignMinMaxOnlyUnsafeNoGaps(inputSegments: Iterable[Segment],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(2 * inputSegments.size, Segment.tempMinMaxKeyValues(inputSegments), targetSegments).map(_.segment)

  def assignMinMaxOnlyUnsafeNoGaps(input: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(2, Segment.tempMinMaxKeyValues(input), targetSegments).map(_.segment)

  def assignMinMaxOnlyUnsafeNoGaps(input: Slice[Memory],
                                   targetSegments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): Iterable[Segment] =
    SegmentAssigner.assignUnsafeNoGaps(2, Segment.tempMinMaxKeyValues(input), targetSegments).map(_.segment)

  def assignUnsafeNoGaps(assignables: Slice[Assignable],
                         segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): ListBuffer[Assignment[Nothing]] =
    assignUnsafe[Nothing](
      assignablesCount = assignables.size,
      assignables = assignables,
      segments = segments,
      noGaps = true
    )

  def assignUnsafeNoGaps(assignablesCount: Int,
                         assignables: Iterable[Assignable],
                         segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]]): ListBuffer[Assignment[Nothing]] =
    assignUnsafe[Nothing](
      assignablesCount = assignablesCount,
      assignables = assignables,
      segments = segments,
      noGaps = true
    )

  def assignUnsafeGaps[GAP](assignables: Slice[Assignable],
                            segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[Assignment[GAP]] =
    assignUnsafe[GAP](
      assignablesCount = assignables.size,
      assignables = assignables,
      segments = segments,
      noGaps = false
    )

  def assignUnsafeGaps[GAP](assignablesCount: Int,
                            assignables: Iterable[Assignable],
                            segments: Iterable[Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                         gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[Assignment[GAP]] =
    assignUnsafe[GAP](
      assignablesCount = assignablesCount,
      assignables = assignables,
      segments = segments,
      noGaps = false
    )

  /**
   * @param assignablesCount keyValuesCount is needed here because keyValues could be a [[ConcurrentSkipList]]
   *                         where calculating size is not constant time.
   */
  private def assignUnsafe[GAP](assignablesCount: Int,
                                assignables: Iterable[Assignable],
                                segments: Iterable[Segment],
                                noGaps: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                 gapCreator: Aggregator.Creator[Assignable, GAP]): ListBuffer[Assignment[GAP]] = {
    import keyOrder._

    val assignments = ListBuffer.empty[Assignment[GAP]]

    val segmentsIterator = segments.iterator

    def getNextSegmentMayBe() = if (segmentsIterator.hasNext) segmentsIterator.next() else Segment.Null

    def assignToSegment(assignable: Assignable,
                        assignTo: Segment): Unit =
      assignments.lastOption match {
        case Some(Assignment(bufferSegment, _, keyValues, _)) if bufferSegment.segmentId == assignTo.segmentId =>
          keyValues += assignable

        case _ =>
          assignments +=
            Assignment(
              segment = assignTo,
              //if noGaps is true then gapCreator will return Nothing Aggregator which cannot be accessed.
              //So use null so that no objects are created.
              headGap = if (noGaps) null else gapCreator.createNew(),
              midOverlap = ListBuffer(assignable),
              tailGap = if (noGaps) null else gapCreator.createNew(),
            )
      }

    def assignToGap(assignable: Assignable,
                    assignTo: Segment): Unit =
      assignments.lastOption match {
        case Some(Assignment(bufferSegment, headGap, keyValues, tailGap)) if bufferSegment.segmentId == assignTo.segmentId =>
          if (keyValues.isEmpty)
            headGap add assignable
          else
            tailGap add assignable

        case _ =>
          val headGap = gapCreator.createNew()
          headGap add assignable

          assignments +=
            Assignment(
              segment = assignTo,
              headGap = headGap,
              midOverlap = ListBuffer.empty,
              tailGap = gapCreator.createNew()
            )
      }

    @tailrec
    def assign(remaining: DropIterator[Memory.Range, Assignable],
               thisSegmentMayBe: SegmentOption,
               nextSegmentMayBe: SegmentOption): Unit = {
      val assignable = remaining.headOrNull

      if (assignable != null)
        thisSegmentMayBe match {
          case Segment.Null =>
            //this would should never occur. If Segments were empty then the key-values should be copied.
            throw new Exception("Cannot assign key-value to Null Segment.")

          case thisSegment: Segment =>
            val keyCompare = keyOrder.compare(assignable.key, thisSegment.minKey)

            //0 = Unknown. 1 = true, -1 = false
            var _belongsTo = 0

            @inline def getKeyBelongsToNoSpread(): Boolean = {
              //avoid computing multiple times.
              if (_belongsTo == 0)
                if (Segment.belongsToNoSpread(assignable, thisSegment))
                  _belongsTo = 1
                else
                  _belongsTo = -1

              _belongsTo == 1
            }

            @inline def spreadToNextSegment(collection: Assignable.Collection, segment: Segment): Boolean =
              collection.maxKey match {
                case MaxKey.Fixed(maxKey) =>
                  maxKey >= segment.minKey

                case MaxKey.Range(_, maxKey) =>
                  maxKey > segment.minKey
              }

            //check if this key-value if it is the new smallest key or if this key belong to this Segment or if there is no next Segment
            if (keyCompare <= 0 || nextSegmentMayBe.isNoneS || getKeyBelongsToNoSpread())
              assignable match {
                case assignable: Assignable.Collection =>
                  nextSegmentMayBe match {
                    case nextSegment: Segment if spreadToNextSegment(assignable, nextSegment) => //check if Segment spreads onto next Segment
                      val keyValueCount = assignable.getKeyValueCount()
                      val keyValues = assignable.iterator()
                      val segmentIterator = DropIterator[Memory.Range, Assignable](keyValueCount, keyValues)

                      val newRemaining = segmentIterator append remaining.dropHead()

                      assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)

                    case _ =>
                      if (noGaps || keyCompare == 0 || getKeyBelongsToNoSpread()) //if this Segment should be added to thisSegment
                        assignToSegment(assignable = assignable, assignTo = thisSegment)
                      else //gap Segment
                        assignToGap(assignable = assignable, assignTo = thisSegment)

                      assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                  }

                case keyValue: KeyValue.Fixed =>
                  if (noGaps || keyCompare == 0 || getKeyBelongsToNoSpread()) //if this key-value should be added to thisSegment
                    assignToSegment(assignable = keyValue, assignTo = thisSegment)
                  else //gap key
                    assignToGap(assignable = keyValue, assignTo = thisSegment)

                  assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)

                case keyValue: KeyValue.Range =>
                  nextSegmentMayBe match {
                    //check if this range key-value spreads onto the next segment
                    case nextSegment: Segment if keyValue.toKey > nextSegment.minKey =>
                      val (fromValue, rangeValue) = keyValue.fetchFromAndRangeValueUnsafe
                      val thisSegmentsRange = Memory.Range(fromKey = keyValue.fromKey, toKey = nextSegment.minKey, fromValue = fromValue, rangeValue = rangeValue)
                      val nextSegmentsRange = Memory.Range(fromKey = nextSegment.minKey, toKey = keyValue.toKey, fromValue = Value.FromValue.Null, rangeValue = rangeValue)

                      if (noGaps || keyCompare == 0 || getKeyBelongsToNoSpread()) //should add to this segment
                        assignToSegment(assignable = thisSegmentsRange, assignTo = thisSegment)
                      else //should add as a gap
                        assignToGap(assignable = thisSegmentsRange, assignTo = thisSegment)

                      assign(remaining.dropPrepend(nextSegmentsRange), nextSegment, getNextSegmentMayBe())

                    case _ =>
                      //belongs to current segment
                      if (noGaps || keyCompare == 0 || getKeyBelongsToNoSpread())
                        assignToSegment(assignable = keyValue, assignTo = thisSegment)
                      else
                        assignToGap(assignable = keyValue, assignTo = thisSegment)

                      assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                  }
              }
            else
              nextSegmentMayBe match {
                case Segment.Null =>
                  if (noGaps)
                    throw new Exception("Cannot assign key-value to Null next Segment.")
                  else
                    remaining.iterator foreach {
                      keyValue =>
                        assignToGap(assignable = keyValue, assignTo = thisSegment)
                    }

                case nextSegment: Segment =>
                  if (assignable.key < nextSegment.minKey) // is this a gap key between thisSegment and the nextSegment
                    assignable match {
                      case assignable: Assignable.Collection =>
                        if (spreadToNextSegment(assignable, nextSegment)) {
                          //if this Segment spreads onto next Segment read all key-values and assign.
                          val keyValueCount = assignable.getKeyValueCount()
                          val keyValues = assignable.iterator()
                          val segmentIterator = DropIterator[Memory.Range, Assignable](keyValueCount, keyValues)

                          val newRemaining = segmentIterator append remaining.dropHead()

                          assign(newRemaining, thisSegmentMayBe, nextSegmentMayBe)

                        } else {
                          //does not spread onto next Segment.
                          if (noGaps || keyCompare == 0 || getKeyBelongsToNoSpread()) //if this Segment should be added to thisSegment
                            assignToSegment(assignable = assignable, assignTo = thisSegment)
                          else //gap segment
                            assignToGap(assignable, thisSegment)

                          assign(remaining.dropHead(), thisSegmentMayBe, nextSegmentMayBe)
                        }

                      case _: KeyValue.Fixed =>
                        if (noGaps) {
                          //check if a key-value is already assigned to thisSegment. Else if thisSegment is empty jump to next
                          //there is no point adding a single key-value to a Segment.
                          assignments.lastOption match {
                            case Some(Assignment(segment, _, keyValues, _)) if segment.segmentId == thisSegment.segmentId =>
                              keyValues += assignable
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
                              case Some(Assignment(segment, _, keyValues, _)) if segment.segmentId == thisSegment.segmentId =>
                                keyValues += keyValue
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
      assign(DropIterator(assignablesCount, assignables.iterator), segmentsIterator.next(), getNextSegmentMayBe())

    assignments
  }
}

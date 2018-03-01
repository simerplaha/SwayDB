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

import swaydb.core.data.KeyValue
import swaydb.data.slice.Slice

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import swaydb.core.util.SliceUtil._

private[core] object SegmentMerge {

  def mergeSmallerSegmentWithPrevious(segments: ListBuffer[Slice[KeyValue]],
                                      minSegmentSize: Long,
                                      forMemory: Boolean,
                                      bloomFilterFalsePositiveRate: Double): ListBuffer[Slice[KeyValue]] =
    if (segments.length >= 2 && ((forMemory && segments.last.memorySegmentSize < minSegmentSize) || segments.last.persistentSegmentSize < minSegmentSize)) {
      val newSegments = segments dropRight 1
      val newSegmentsLast = newSegments.last
      segments.last foreach {
        keyValue =>
          newSegmentsLast add keyValue.updateStats(bloomFilterFalsePositiveRate, newSegmentsLast.lastOption)
      }
      newSegments.map(_.close())
    } else
      segments.map(_.close())

  def add(nextKeyValue: KeyValue,
          remainingKeyValues: Int,
          segmentKeyValues: ListBuffer[Slice[KeyValue]],
          minSegmentSize: Long,
          forInMemory: Boolean,
          bloomFilterFalsePositiveRate: Double): Unit = {
    val currentSplitsLastKeyValue = segmentKeyValues.lastOption.flatMap(_.lastOption)

    val currentSegmentSize =
      if (forInMemory)
        currentSplitsLastKeyValue.map(_.stats.memorySegmentSize).getOrElse(0)
      else
        currentSplitsLastKeyValue.map(_.stats.segmentSize).getOrElse(0)

    val nextKeyValueWithUpdatedStats = nextKeyValue.updateStats(bloomFilterFalsePositiveRate, currentSplitsLastKeyValue)

    val segmentSizeWithNextKeyValue =
      if (forInMemory)
        currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValueMemorySize
      else
        currentSegmentSize + nextKeyValueWithUpdatedStats.stats.thisKeyValuesSegmentSizeWithoutFooter

    def startNewSegment(): Unit =
      segmentKeyValues += Slice.create[KeyValue](remainingKeyValues)

    def addKeyValue(): Unit =
      segmentKeyValues.last add nextKeyValueWithUpdatedStats

    if (segmentSizeWithNextKeyValue >= minSegmentSize) {
      addKeyValue()
      startNewSegment()
    } else
      addKeyValue()
  }

  def split(keyValues: Slice[KeyValue],
            minSegmentSize: Long,
            removeDeletes: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Iterable[Slice[KeyValue]] =
    merge(keyValues, Slice.create[KeyValue](0), minSegmentSize, removeDeletes = removeDeletes, forInMemory = forInMemory, bloomFilterFalsePositiveRate)

  def merge(newKeyValues: Slice[KeyValue],
            currentKeyValues: Slice[KeyValue],
            minSegmentSize: Long,
            removeDeletes: Boolean,
            forInMemory: Boolean,
            bloomFilterFalsePositiveRate: Double)(implicit ordering: Ordering[Slice[Byte]]): Iterable[Slice[KeyValue]] = {
    import ordering._
    val splits = ListBuffer[Slice[KeyValue]](Slice.create[KeyValue](newKeyValues.size + currentKeyValues.size))

    @tailrec
    def doMerge(newKeyValues: Slice[KeyValue],
                oldKeyValues: Slice[KeyValue]): ListBuffer[Slice[KeyValue]] = {
      //used to create the new split Slice for new split segment.
      def remainingKeyValuesCount = newKeyValues.size + currentKeyValues.size

      (newKeyValues.headOption, oldKeyValues.headOption) match {

        case (Some(newKeyValue), Some(oldKeyValue)) if oldKeyValue.key < newKeyValue.key =>
          if (!(removeDeletes && oldKeyValue.isRemove))
            add(oldKeyValue, remainingKeyValuesCount - 1, splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)

          doMerge(newKeyValues, oldKeyValues.drop(1))

        case (Some(newKeyValue), Some(oldKeyValue)) if newKeyValue.key < oldKeyValue.key =>
          if (!(removeDeletes && newKeyValue.isRemove))
            add(newKeyValue, remainingKeyValuesCount - 1, splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)

          doMerge(newKeyValues.drop(1), oldKeyValues)

        case (Some(newKeyValue), Some(_)) => //equals
          if (removeDeletes && newKeyValue.isRemove) {
            doMerge(newKeyValues.drop(1), oldKeyValues.drop(1))
          } else {
            add(newKeyValue, remainingKeyValuesCount - 2, splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)
            doMerge(newKeyValues.drop(1), oldKeyValues.drop(1))
          }

        //there are no more oldKeyValues. Add all remaining newKeyValues
        case (Some(_), None) =>
          newKeyValues foreach {
            keyValue =>
              if (!(removeDeletes && keyValue.isRemove))
                add(keyValue, remainingKeyValuesCount, splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)
          }
          mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)

        //there are no more newKeyValues. Add all remaining oldKeyValues
        case (None, Some(_)) =>
          oldKeyValues foreach {
            keyValue =>
              if (!(removeDeletes && keyValue.isRemove))
                add(keyValue, remainingKeyValuesCount, splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)
          }
          mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)

        case (None, None) =>
          mergeSmallerSegmentWithPrevious(splits, minSegmentSize, forInMemory, bloomFilterFalsePositiveRate)
      }
    }


    val newSlices =
      if (newKeyValues.isEmpty && currentKeyValues.isEmpty)
        Seq(currentKeyValues)
      else
        doMerge(newKeyValues, currentKeyValues)

    newSlices.filter(_.nonEmpty)
  }

}

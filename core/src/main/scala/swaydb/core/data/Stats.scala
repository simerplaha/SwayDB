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

package swaydb.core.data

import swaydb.core.segment.format.a.SegmentWriter
import swaydb.core.util.{BloomFilter, Bytes}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline

private[core] object Stats {

  def apply(key: Slice[Byte],
            value: Option[Slice[Byte]],
            falsePositiveRate: Double,
            isRemoveRange: Boolean,
            isRange: Boolean,
            isGroup: Boolean,
            isPut: Boolean,
            bloomFiltersItemCount: Int,
            previous: Option[KeyValue.WriteOnly],
            deadline: Option[Deadline]): Stats = {

    val valueLength =
      value.map(_.size).getOrElse(0)

    val hasRemoveRange =
      previous.exists(_.stats.hasRemoveRange) || isRemoveRange

    val hasPut =
      previous.exists(_.stats.hasPut) || isPut

    val hasRange =
      previous.exists(_.stats.hasRange) || isRemoveRange || isRange

    val previousStats =
      previous.map(_.stats)

    val position =
      previousStats.map(_.position + 1) getOrElse 1

    val numberOfRanges =
      if (isRange)
        previousStats.map(_.numberOfRanges + 1) getOrElse 1
      else
        previousStats.map(_.numberOfRanges) getOrElse 0

    val groupsCount =
      if (isGroup)
        previousStats.map(_.groupsCount + 1) getOrElse 1
      else
        previousStats.map(_.groupsCount) getOrElse 0

    //Items to add to BloomFilters is different to the position because a Group can contain
    //multiple inner key-values but the Group's key itself does not get added to the BloomFilter.
    val totalBloomFiltersItemsCount =
    previousStats.map(_.bloomFilterKeysCount + bloomFiltersItemCount) getOrElse bloomFiltersItemCount

    val thisKeyValuesIndexSizeWithoutFooter =
      Bytes.sizeOf(key.size) + key.size

    val thisKeyValuesSegmentSizeWithoutFooter: Int =
      thisKeyValuesIndexSizeWithoutFooter + valueLength

    val thisKeyValuesIndexOffset =
      previousStats map {
        previous =>
          previous.thisKeyValuesIndexOffset + previous.thisKeyValuesIndexSizeWithoutFooter
      } getOrElse 0

    val segmentSizeWithoutFooter: Int =
      previousStats.map(_.segmentSizeWithoutFooter).getOrElse(0) + thisKeyValuesSegmentSizeWithoutFooter

    //calculates the size of Segment after the last Group. This is used for size based grouping/compression.
    val segmentSizeWithoutFooterForNextGroup: Int =
      if (previous.exists(_.isGroup)) //if previous is a group, restart the size calculation
        thisKeyValuesSegmentSizeWithoutFooter
      else //if previous is not a group, add previous key-values set segment size since the last group to this key-values Segment size.
        previousStats.map(_.segmentSizeWithoutFooterForNextGroup).getOrElse(0) + thisKeyValuesSegmentSizeWithoutFooter

    val segmentValuesSize: Int =
      previousStats.map(_.segmentValuesSize).getOrElse(0) + valueLength

    val footerSize =
      Bytes.sizeOf(SegmentWriter.formatId) + //1 byte for format
        1 + //for created in level
        1 + //for isGrouped
        1 + //for hasRange
        1 + //for hasPut
        ByteSizeOf.long + //for CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
        Bytes.sizeOf(segmentValuesSize) + //index offset
        Bytes.sizeOf(position) + //key-values count
        Bytes.sizeOf(totalBloomFiltersItemsCount) + {
        //        if (hasRemoveRange) { //BloomFilter is not created when hasRemoveRange is true.
        //          1 //(1 for unsigned int 0)
        //        } else {
        val bloomFilterByteSize = BloomFilter.optimalSegmentBloomFilterByteSize(totalBloomFiltersItemsCount, falsePositiveRate)
        Bytes.sizeOf(bloomFilterByteSize) + bloomFilterByteSize
        //        }
      } + {
        val rangeFilterByteSize = BloomFilter.optimalRangeFilterByteSize(totalBloomFiltersItemsCount)
        Bytes.sizeOf(rangeFilterByteSize) + rangeFilterByteSize
      }

    val segmentSize: Int =
      segmentSizeWithoutFooter + footerSize + ByteSizeOf.int

    val segmentUncompressedKeysSize: Int =
      previousStats.map(_.segmentUncompressedKeysSize).getOrElse(0) + key.size

    new Stats(
      valueLength = valueLength,
      segmentSize = segmentSize,
      position = position,
      segmentValuesSize = segmentValuesSize,
      segmentUncompressedKeysSize = segmentUncompressedKeysSize,
      segmentSizeWithoutFooter = segmentSizeWithoutFooter,
      segmentSizeWithoutFooterForNextGroup = segmentSizeWithoutFooterForNextGroup,
      keySize = key.size,
      thisKeyValuesSegmentSizeWithoutFooter = thisKeyValuesSegmentSizeWithoutFooter,
      thisKeyValuesIndexSizeWithoutFooter = thisKeyValuesIndexSizeWithoutFooter,
      thisKeyValuesIndexOffset = thisKeyValuesIndexOffset,
      hasRemoveRange = hasRemoveRange,
      numberOfRanges = numberOfRanges,
      bloomFilterKeysCount = totalBloomFiltersItemsCount,
      groupsCount = groupsCount,
      hasPut = hasPut,
      hasRange = hasRange,
      isGroup = isGroup
    )
  }
}

private[core] case class Stats(valueLength: Int,
                               segmentSize: Int,
                               position: Int,
                               groupsCount: Int,
                               bloomFilterKeysCount: Int,
                               segmentValuesSize: Int,
                               segmentUncompressedKeysSize: Int,
                               segmentSizeWithoutFooter: Int,
                               segmentSizeWithoutFooterForNextGroup: Int,
                               keySize: Int,
                               thisKeyValuesSegmentSizeWithoutFooter: Int,
                               thisKeyValuesIndexSizeWithoutFooter: Int,
                               thisKeyValuesIndexOffset: Int,
                               numberOfRanges: Int,
                               hasRemoveRange: Boolean,
                               hasRange: Boolean,
                               hasPut: Boolean,
                               isGroup: Boolean) {
  def isNoneValue: Boolean =
    valueLength == 0

  def hasGroup: Boolean =
    groupsCount > 0

  def memorySegmentSize =
    segmentUncompressedKeysSize + segmentValuesSize

  def thisKeyValueMemorySize =
    keySize + valueLength

  def indexSize =
    segmentSizeWithoutFooter - segmentValuesSize
}

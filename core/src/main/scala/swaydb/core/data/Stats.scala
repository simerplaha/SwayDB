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

import swaydb.core.segment.format.a.{SegmentHashIndex, SegmentWriter}
import swaydb.core.util.{BloomFilter, Bytes}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.collection.{SortedSet, immutable}
import scala.concurrent.duration.Deadline

private[core] object Stats {

  def createRangeCommonPrefixesCount(int: Int) =
    immutable.SortedSet[Int](int)(Ordering.Int.reverse)

  val emptyRangeCommonPrefixesCount =
    immutable.SortedSet.empty[Int](Ordering.Int.reverse)

  def apply(indexEntry: Slice[Byte],
            value: Option[Slice[Byte]],
            falsePositiveRate: Double,
            isRemoveRange: Boolean,
            isRange: Boolean,
            isGroup: Boolean,
            isPut: Boolean,
            position: Int,
            hashIndexItemsCount: Int, //position accounting for Groups.
            numberOfRanges: Int,
            bloomFiltersItemCount: Int,
            usePreviousHashIndexOffset: Boolean,
            minimumNumberOfKeysForHashIndex: Int,
            hashIndexCompensation: Int => Int,
            rangeCommonPrefixesCount: SortedSet[Int],
            enableRangeFilterAndIndex: Boolean,
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

    val totalNumberOfRanges =
      previousStats.map(_.totalNumberOfRanges + numberOfRanges) getOrElse numberOfRanges

    val groupsCount =
      if (isGroup)
        previousStats.map(_.groupsCount + 1) getOrElse 1
      else
        previousStats.map(_.groupsCount) getOrElse 0

    val thisKeyValuesIndexSizeWithoutFooter =
      Bytes.sizeOf(indexEntry.size) + indexEntry.size

    val thisKeyValueIndexOffset =
      previousStats map {
        previous =>
          previous.thisKeyValueIndexOffset + previous.thisKeyValuesIndexSizeWithoutFooter
      } getOrElse 0

    //starts from 0. Do not need the actual index offset for space efficiency. The actual indexOffset can be adjust during read.
    val thisKeyValuesHashIndexesSortedIndexOffset =
      if (usePreviousHashIndexOffset)
        previousStats.map(_.thisKeyValuesHashIndexesSortedIndexOffset) getOrElse thisKeyValueIndexOffset
      else
        thisKeyValueIndexOffset

    //Items to add to BloomFilters is different to the position because a Group can contain
    //multiple inner key-values but the Group's key itself does not get added to the BloomFilter.
    val totalBloomFiltersItemsCount =
    previousStats.map(_.totalBloomFiltersItemsCount + bloomFiltersItemCount) getOrElse bloomFiltersItemCount

    val thisKeyValuesSegmentSizeWithoutFooterAndHashIndex: Int =
      thisKeyValuesIndexSizeWithoutFooter +
        valueLength

    val segmentHashIndexSize =
      SegmentHashIndex.optimalBytesRequired(
        hashIndexItemsCount = hashIndexItemsCount,
        largestSortedIndexOffset = thisKeyValuesHashIndexesSortedIndexOffset,
        minimumNumberOfKeyValues = minimumNumberOfKeysForHashIndex,
        compensate = hashIndexCompensation
      )

    val segmentSizeWithoutFooter: Int =
      previousStats.map(previous => previous.segmentSizeWithoutFooter - previous.segmentHashIndexSize).getOrElse(0) +
        thisKeyValuesSegmentSizeWithoutFooterAndHashIndex +
        segmentHashIndexSize

    //calculates the size of Segment after the last Group. This is used for size based grouping/compression.
    val segmentSizeWithoutFooterForNextGroup: Int =
      if (previous.exists(_.isGroup)) //if previous is a group, restart the size calculation
        thisKeyValuesSegmentSizeWithoutFooterAndHashIndex + segmentHashIndexSize
      else //if previous is not a group, add previous key-values set segment size since the last group to this key-values Segment size.
        previousStats.map(_.segmentSizeWithoutFooterForNextGroup).getOrElse(0) +
          thisKeyValuesSegmentSizeWithoutFooterAndHashIndex +
          segmentHashIndexSize

    val segmentValuesSize: Int =
      previousStats.map(_.segmentValuesSize).getOrElse(0) + valueLength

    val segmentIndexSize =
      previousStats.map(_.segmentIndexSize).getOrElse(0) + thisKeyValuesIndexSizeWithoutFooter

    val optimalRangeFilterSize = BloomFilter.optimalRangeFilterByteSize(enableRangeFilterAndIndex, totalNumberOfRanges, rangeCommonPrefixesCount)
    val optimalBloomFilterSize = BloomFilter.optimalSegmentBloomFilterByteSize(totalBloomFiltersItemsCount, falsePositiveRate)

    val footerHeaderSize =
      Bytes.sizeOf(SegmentWriter.formatId) + //1 byte for format
        1 + //created in level
        1 + //isGrouped
        1 + //hasRange
        1 + //hasPut
        ByteSizeOf.long + //for CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
        Bytes.sizeOf(segmentValuesSize max 0) + //index offset.
        Bytes.sizeOf((segmentValuesSize + segmentIndexSize) max 1) + //hash index offset. HashIndex offset will never be 0 since that's reserved for index is values are none..
        Bytes.sizeOf(position) + //key-values count
        Bytes.sizeOf(totalBloomFiltersItemsCount) +
        ByteSizeOf.int + //Size of optimalBloomFilterSize. Cannot be an unsigned int because optimalBloomFilterSize can be larger than actual.
        optimalBloomFilterSize +
        Bytes.sizeOf(optimalRangeFilterSize) +
        optimalRangeFilterSize

    val segmentSize: Int =
      segmentSizeWithoutFooter +
        footerHeaderSize +
        ByteSizeOf.int //to store footer offset.

    val segmentUncompressedKeysSize: Int =
      previousStats.map(_.segmentUncompressedKeysSize).getOrElse(0) + indexEntry.size

    new Stats(
      valueLength = valueLength,
      segmentSize = segmentSize,
      position = position,
      hashIndexItemsCount = hashIndexItemsCount,
      groupsCount = groupsCount,
      totalBloomFiltersItemsCount = totalBloomFiltersItemsCount,
      segmentValuesSize = segmentValuesSize,
      segmentIndexSize = segmentIndexSize,
      segmentUncompressedKeysSize = segmentUncompressedKeysSize,
      segmentSizeWithoutFooter = segmentSizeWithoutFooter,
      segmentSizeWithoutFooterForNextGroup = segmentSizeWithoutFooterForNextGroup,
      keySize = indexEntry.size,
      thisKeyValuesSegmentSizeWithoutFooterAndHashIndex = thisKeyValuesSegmentSizeWithoutFooterAndHashIndex,
      thisKeyValuesIndexSizeWithoutFooter = thisKeyValuesIndexSizeWithoutFooter,
      thisKeyValuesHashIndexesSortedIndexOffset = thisKeyValuesHashIndexesSortedIndexOffset,
      thisKeyValueIndexOffset = thisKeyValueIndexOffset,
      segmentHashIndexSize = segmentHashIndexSize,
      bloomFilterSize = optimalBloomFilterSize,
      rangeFilterSize = optimalRangeFilterSize,
      footerHeaderSize = footerHeaderSize,
      totalNumberOfRanges = totalNumberOfRanges,
      hasRemoveRange = hasRemoveRange,
      hasRange = hasRange,
      hasPut = hasPut,
      isGroup = isGroup,
      rangeCommonPrefixesCount = rangeCommonPrefixesCount
    )
  }
}

private[core] case class Stats(valueLength: Int,
                               segmentSize: Int,
                               position: Int,
                               hashIndexItemsCount: Int,
                               groupsCount: Int,
                               totalBloomFiltersItemsCount: Int,
                               segmentValuesSize: Int,
                               segmentIndexSize: Int,
                               segmentUncompressedKeysSize: Int,
                               segmentSizeWithoutFooter: Int,
                               segmentSizeWithoutFooterForNextGroup: Int,
                               keySize: Int,
                               thisKeyValuesSegmentSizeWithoutFooterAndHashIndex: Int,
                               thisKeyValuesIndexSizeWithoutFooter: Int,
                               thisKeyValuesHashIndexesSortedIndexOffset: Int,
                               thisKeyValueIndexOffset: Int,
                               segmentHashIndexSize: Int,
                               bloomFilterSize: Int,
                               rangeFilterSize: Int,
                               footerHeaderSize: Int,
                               totalNumberOfRanges: Int,
                               hasRemoveRange: Boolean,
                               hasRange: Boolean,
                               hasPut: Boolean,
                               isGroup: Boolean,
                               rangeCommonPrefixesCount: SortedSet[Int]) {
  def isNoneValue: Boolean =
    valueLength == 0

  def hasGroup: Boolean =
    groupsCount > 0

  def memorySegmentSize =
    segmentUncompressedKeysSize + segmentValuesSize

  def thisKeyValueMemorySize =
    keySize + valueLength

  def sortedIndexSize =
    segmentSizeWithoutFooter - segmentHashIndexSize - segmentValuesSize
}

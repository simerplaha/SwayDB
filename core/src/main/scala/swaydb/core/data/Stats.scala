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

import swaydb.core.segment.format.a.block._
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline

private[core] object Stats {

  val segmentFooterSize =
    ByteSizeOf.byte + //1 byte for format
      ByteSizeOf.varInt + //created in level
      ByteSizeOf.boolean + //hasGroup
      ByteSizeOf.boolean + //hasRange
      ByteSizeOf.boolean + //hasPut
      ByteSizeOf.varInt + //key-values count
      ByteSizeOf.varInt + //uniqueKeysCount
      ByteSizeOf.long + //CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
      ByteSizeOf.varInt + //sorted index offset.
      ByteSizeOf.varInt + //sorted index size.
      ByteSizeOf.varInt + //hash index offset. HashIndex offset will never be 0 since that's reserved for index is values are none..
      ByteSizeOf.varInt + //hash index size.
      ByteSizeOf.varInt + //binarySearch index
      ByteSizeOf.varInt + //binary index size.
      ByteSizeOf.varInt + //bloomFilter
      ByteSizeOf.varInt + //bloomFilter
      ByteSizeOf.varInt //footer offset.

  def apply(keySize: Int,
            indexEntry: Slice[Byte],
            value: Slice[Slice[Byte]],
            isRemoveRange: Boolean,
            isRange: Boolean,
            isGroup: Boolean,
            isPut: Boolean,
            isPrefixCompressed: Boolean,
            thisKeyValuesNumberOfRanges: Int,
            thisKeyValuesUniqueKeys: Int,
            sortedIndex: SortedIndex.Config,
            bloomFilter: BloomFilter.Config,
            hashIndex: HashIndex.Config,
            binarySearch: BinarySearchIndex.Config,
            values: Values.Config,
            previousStats: Option[Stats],
            deadline: Option[Deadline]): Stats = {

    val valueLength =
      value.foldLeft(0)(_ + _.size)

    val hasRemoveRange =
      previousStats.exists(_.segmentHasRemoveRange) || isRemoveRange

    val chainPosition =
      previousStats.map(_.chainPosition + 1) getOrElse 1

    val groupsCount =
      if (isGroup)
        previousStats.map(_.groupsCount + 1) getOrElse 1
      else
        previousStats.map(_.groupsCount) getOrElse 0

    val thisKeyValueAccessIndexPosition =
      if (sortedIndex.enableAccessPositionIndex)
        if (isPrefixCompressed)
          previousStats.map(_.thisKeyValueAccessIndexPosition) getOrElse 1
        else
          previousStats.map(_.thisKeyValueAccessIndexPosition + 1) getOrElse 1
      else
        0

    val thisKeyValueAccessIndexPositionByteSize =
      if (sortedIndex.enableAccessPositionIndex)
        Bytes.sizeOf(thisKeyValueAccessIndexPosition)
      else
        0

    val thisKeyValuesSortedIndexSize =
      Bytes.sizeOf(indexEntry.size + thisKeyValueAccessIndexPositionByteSize) +
        thisKeyValueAccessIndexPositionByteSize +
        indexEntry.size

    val thisKeyValuesSortedIndexSizeWithoutFooter =
      SortedIndex.headerSize(sortedIndex.compressions.nonEmpty) +
        thisKeyValuesSortedIndexSize

    val thisKeyValuesRealIndexOffset =
      previousStats map {
        previous =>
          previous.thisKeyValueRealIndexOffset + previous.thisKeyValuesSortedIndexSize
      } getOrElse 0

    //starts from 0. Do not need the actual index offset for space efficiency. The actual indexOffset can be adjust during read.
    val thisKeyValuesAccessIndexOffset =
      if (isPrefixCompressed)
        previousStats.map(_.thisKeyValuesAccessIndexOffset) getOrElse thisKeyValuesRealIndexOffset
      else
        thisKeyValuesRealIndexOffset

    val thisKeyValuesSegmentValueSize =
      if (valueLength == 0)
        0
      else
        Values.headerSize(values.compressions.nonEmpty) +
          valueLength

    val thisKeyValuesSegmentSortedIndexAndValueSize =
      thisKeyValuesSegmentValueSize +
        thisKeyValuesSortedIndexSizeWithoutFooter

    val segmentHasRange =
      hasRemoveRange || previousStats.exists(_.segmentHasRange) || isRange

    val segmentHasPut =
      previousStats.exists(_.segmentHasPut) || isPut

    val segmentTotalNumberOfRanges =
      previousStats.map(_.segmentTotalNumberOfRanges + thisKeyValuesNumberOfRanges) getOrElse thisKeyValuesNumberOfRanges

    //Items to add to BloomFilters is different to the position because a Group can contain
    //multiple inner key-values but the Group's key itself does not get added to the BloomFilter.
    val segmentUniqueKeysCount =
    previousStats.map(_.segmentUniqueKeysCount + thisKeyValuesUniqueKeys) getOrElse thisKeyValuesUniqueKeys

    //unique keys that do not have prefix compressed keys.
    val segmentUniqueAccessIndexKeyCounts =
      if (isPrefixCompressed)
        previousStats.map(_.segmentUniqueAccessIndexKeyCounts) getOrElse 1
      else
        previousStats.map(_.segmentUniqueAccessIndexKeyCounts + 1) getOrElse 1

    val segmentHashIndexSize =
      if (segmentUniqueKeysCount < hashIndex.minimumNumberOfKeys)
        0
      else
        HashIndex.optimalBytesRequired(
          keyCounts = segmentUniqueKeysCount,
          minimumNumberOfKeys = hashIndex.minimumNumberOfKeys,
          largestValue = thisKeyValuesAccessIndexOffset,
          allocateSpace = hashIndex.allocateSpace,
          hasCompression = hashIndex.compressions.nonEmpty
        )

    //binary search indexes are only created for non-prefix compressed or reset point keys.
    //size calculation should only account for those entries because duplicates are not allowed.
    def binarySearchIndexEntriesCount() =
      if (binarySearch.fullIndex)
        segmentUniqueAccessIndexKeyCounts
      else
        segmentTotalNumberOfRanges

    val segmentBinarySearchIndexSize =
      if (binarySearch.enabled)
        previousStats flatMap {
          previousStats =>
            if (previousStats.thisKeyValuesAccessIndexOffset == thisKeyValuesAccessIndexOffset)
              Some(previousStats.segmentBinarySearchIndexSize)
            else
              None
        } getOrElse {
          BinarySearchIndex.optimalBytesRequired(
            largestValue = thisKeyValuesAccessIndexOffset,
            hasCompression = binarySearch.compressions.nonEmpty,
            minimNumberOfKeysForBinarySearchIndex = binarySearch.minimumNumberOfKeys,
            valuesCount = binarySearchIndexEntriesCount()
          )
        }
      else
        0

    val segmentValuesSizeWithoutHeader: Int =
      previousStats.map(_.segmentValuesSizeWithoutHeader).getOrElse(0) +
        valueLength

    val segmentValuesSize: Int =
      if (segmentValuesSizeWithoutHeader != 0)
        Values.headerSize(values.compressions.nonEmpty) +
          segmentValuesSizeWithoutHeader
      else if (valueLength != 0)
        Values.headerSize(values.compressions.nonEmpty) +
          segmentValuesSizeWithoutHeader
      else
        0

    val segmentSortedIndexSizeWithoutHeader =
      previousStats.map(_.segmentSortedIndexSizeWithoutHeader).getOrElse(0) +
        thisKeyValuesSortedIndexSize

    val segmentSortedIndexSize =
      SortedIndex.headerSize(sortedIndex.compressions.nonEmpty) +
        segmentSortedIndexSizeWithoutHeader

    val segmentValueAndSortedIndexEntrySize =
      if (segmentValuesSizeWithoutHeader == 0)
        segmentSortedIndexSizeWithoutHeader +
          SortedIndex.headerSize(sortedIndex.compressions.nonEmpty)
      else
        segmentValuesSizeWithoutHeader +
          segmentSortedIndexSizeWithoutHeader +
          SortedIndex.headerSize(sortedIndex.compressions.nonEmpty) +
          Values.headerSize(values.compressions.nonEmpty)

    val segmentBloomFilterSize =
      if (bloomFilter.falsePositiveRate <= 0.0 || hasRemoveRange || segmentUniqueKeysCount < bloomFilter.minimumNumberOfKeys)
        0
      else
        BloomFilter.optimalSize(
          numberOfKeys = segmentUniqueKeysCount,
          falsePositiveRate = bloomFilter.falsePositiveRate,
          hasCompression = bloomFilter.compressions.nonEmpty,
          minimumNumberOfKeys = bloomFilter.minimumNumberOfKeys
        )

    val segmentSizeWithoutFooter: Int =
      segmentValuesSize +
        segmentSortedIndexSize +
        segmentHashIndexSize +
        segmentBinarySearchIndexSize +
        segmentBloomFilterSize

    //calculates the size of Segment after the last Group. This is used for size based grouping/compression.
    val segmentSizeWithoutFooterForNextGroup: Int =
      if (previousStats.exists(_.isGroup)) //if previous is a group, restart the size calculation
        segmentSizeWithoutFooter
      else //if previous is not a group, add previous key-values set segment size since the last group to this key-values Segment size.
        previousStats.map(_.segmentSizeWithoutFooterForNextGroup).getOrElse(0) +
          segmentSizeWithoutFooter

    val segmentSize: Int =
      segmentSizeWithoutFooter +
        segmentFooterSize

    val segmentUncompressedKeysSize: Int =
      previousStats.map(_.segmentUncompressedKeysSize).getOrElse(0) + keySize

    new Stats(
      valueLength = valueLength,
      segmentSize = segmentSize,
      chainPosition = chainPosition,
      segmentValueAndSortedIndexEntrySize = segmentValueAndSortedIndexEntrySize,
      segmentSortedIndexSizeWithoutHeader = segmentSortedIndexSizeWithoutHeader,
      thisKeyValueAccessIndexPositionByteSize = thisKeyValueAccessIndexPositionByteSize,
      groupsCount = groupsCount,
      segmentUniqueKeysCount = segmentUniqueKeysCount,
      segmentValuesSize = segmentValuesSize,
      segmentValuesSizeWithoutHeader = segmentValuesSizeWithoutHeader,
      segmentSortedIndexSize = segmentSortedIndexSize,
      segmentUncompressedKeysSize = segmentUncompressedKeysSize,
      segmentSizeWithoutFooter = segmentSizeWithoutFooter,
      segmentSizeWithoutFooterForNextGroup = segmentSizeWithoutFooterForNextGroup,
      segmentUniqueAccessIndexKeyCounts = segmentUniqueAccessIndexKeyCounts,
      thisKeyValuesSegmentKeyAndValueSize = thisKeyValuesSegmentSortedIndexAndValueSize,
      thisKeyValuesSortedIndexSize = thisKeyValuesSortedIndexSize,
      thisKeyValuesAccessIndexOffset = thisKeyValuesAccessIndexOffset,
      thisKeyValueRealIndexOffset = thisKeyValuesRealIndexOffset,
      thisKeyValueAccessIndexPosition = thisKeyValueAccessIndexPosition,
      segmentHashIndexSize = segmentHashIndexSize,
      segmentBloomFilterSize = segmentBloomFilterSize,
      segmentBinarySearchIndexSize = segmentBinarySearchIndexSize,
      segmentTotalNumberOfRanges = segmentTotalNumberOfRanges,
      segmentHasRemoveRange = hasRemoveRange,
      segmentHasRange = segmentHasRange,
      segmentHasPut = segmentHasPut,
      isGroup = isGroup
    )
  }
}

private[core] case class Stats(valueLength: Int,
                               segmentSize: Int,
                               chainPosition: Int,
                               segmentValueAndSortedIndexEntrySize: Int,
                               segmentSortedIndexSizeWithoutHeader: Int,
                               groupsCount: Int,
                               segmentUniqueKeysCount: Int,
                               segmentValuesSize: Int,
                               segmentValuesSizeWithoutHeader: Int,
                               segmentSortedIndexSize: Int,
                               segmentUncompressedKeysSize: Int,
                               segmentSizeWithoutFooter: Int,
                               segmentSizeWithoutFooterForNextGroup: Int,
                               segmentUniqueAccessIndexKeyCounts: Int,
                               thisKeyValuesSegmentKeyAndValueSize: Int,
                               thisKeyValuesSortedIndexSize: Int,
                               thisKeyValuesAccessIndexOffset: Int,
                               private[data] val thisKeyValueRealIndexOffset: Int,
                               thisKeyValueAccessIndexPositionByteSize: Int,
                               thisKeyValueAccessIndexPosition: Int,
                               segmentHashIndexSize: Int,
                               segmentBloomFilterSize: Int,
                               segmentBinarySearchIndexSize: Int,
                               segmentTotalNumberOfRanges: Int,
                               segmentHasRemoveRange: Boolean,
                               segmentHasRange: Boolean,
                               segmentHasPut: Boolean,
                               isGroup: Boolean) {
  def segmentHasGroup: Boolean =
    groupsCount > 0

  def memorySegmentSize =
    segmentUncompressedKeysSize + segmentValuesSize

  def thisKeyValueMemorySize =
    thisKeyValuesSortedIndexSize + valueLength
}

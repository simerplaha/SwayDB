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

import swaydb.core.segment.format.a.SegmentBlock
import swaydb.core.segment.format.a.block._
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline

private[core] object Stats {

  def apply(keySize: Int,
            indexEntry: Slice[Byte],
            value: Slice[Slice[Byte]],
            isRemoveRange: Boolean,
            isRange: Boolean,
            isGroup: Boolean,
            isPut: Boolean,
            isPrefixCompressed: Boolean,
            numberOfRanges: Int,
            thisKeyValuesUniqueKeys: Int,
            sortedIndex: SortedIndex.Config,
            bloomFilter: BloomFilter.Config,
            hashIndex: HashIndex.Config,
            binarySearch: BinarySearchIndex.Config,
            values: Values.Config,
            previous: Option[KeyValue.WriteOnly],
            deadline: Option[Deadline]): Stats = {

    val previousStats =
      previous.map(_.stats)

    val valueLength =
      value.foldLeft(0)(_ + _.size)

    val hasRemoveRange =
      previous.exists(_.stats.segmentHasRemoveRange) || isRemoveRange

    val segmentHasRange =
      hasRemoveRange || previous.exists(_.stats.segmentHasRange) || isRange

    val segmentHasPut =
      previous.exists(_.stats.segmentHasPut) || isPut

    val chainPosition =
      previousStats.map(_.chainPosition + 1) getOrElse 1

    val segmentTotalNumberOfRanges =
      previousStats.map(_.segmentTotalNumberOfRanges + numberOfRanges) getOrElse numberOfRanges

    val groupsCount =
      if (isGroup)
        previousStats.map(_.groupsCount + 1) getOrElse 1
      else
        previousStats.map(_.groupsCount) getOrElse 0

    val thisKeyValuesSortedIndexSizeWithoutFooterAndHeader =
      Bytes.sizeOf(indexEntry.size) +
        indexEntry.size

    val thisKeyValuesSortedIndexSizeWithoutFooter =
      SortedIndex.headerSize(sortedIndex.hasCompression) +
        thisKeyValuesSortedIndexSizeWithoutFooterAndHeader

    val thisKeyValuesRealIndexOffset =
      previousStats map {
        previous =>
          previous.thisKeyValueIndexOffset + previous.thisKeyValuesIndexSizeWithoutFooterAndHeader
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
        Values.headerSize(values.hasCompression) +
          valueLength

    val thisKeyValuesSegmentKeyAndValueSize =
      thisKeyValuesSegmentValueSize +
        thisKeyValuesSortedIndexSizeWithoutFooter

    //Items to add to BloomFilters is different to the position because a Group can contain
    //multiple inner key-values but the Group's key itself does not find added to the BloomFilter.
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
          largestValue = thisKeyValuesAccessIndexOffset,
          allocateSpace = hashIndex.allocateSpace,
          hasCompression = hashIndex.hasCompression
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
        previousStats map {
          previousStats =>
            if (previousStats.thisKeyValuesAccessIndexOffset != thisKeyValuesAccessIndexOffset)
              BinarySearchIndex.optimalBytesRequired(
                largestValue = thisKeyValuesAccessIndexOffset,
                hasCompression = binarySearch.hasCompression,
                minimNumberOfKeysForBinarySearchIndex = binarySearch.minimumNumberOfKeys,
                valuesCount = binarySearchIndexEntriesCount()
              )
            else
              previousStats.segmentBinarySearchIndexSize
        } getOrElse {
          BinarySearchIndex.optimalBytesRequired(
            largestValue = thisKeyValuesAccessIndexOffset,
            hasCompression = binarySearch.hasCompression,
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
        Values.headerSize(values.hasCompression) +
          segmentValuesSizeWithoutHeader
      else if (valueLength != 0)
        Values.headerSize(values.hasCompression) +
          segmentValuesSizeWithoutHeader
      else
        0

    val segmentSortedIndexSizeWithoutHeader =
      previousStats.map(_.segmentSortedIndexSizeWithoutHeader).getOrElse(0) +
        thisKeyValuesSortedIndexSizeWithoutFooterAndHeader

    val segmentSortedIndexSize =
      SortedIndex.headerSize(sortedIndex.hasCompression) +
        segmentSortedIndexSizeWithoutHeader

    val segmentValueAndSortedIndexEntrySize =
      if (segmentValuesSizeWithoutHeader == 0)
        segmentSortedIndexSizeWithoutHeader +
          SortedIndex.headerSize(sortedIndex.hasCompression)
      else
        segmentValuesSizeWithoutHeader +
          segmentSortedIndexSizeWithoutHeader +
          SortedIndex.headerSize(sortedIndex.hasCompression) +
          Values.headerSize(values.hasCompression)

    val segmentBloomFilterSize =
      if (bloomFilter.falsePositiveRate <= 0.0 || (hasRemoveRange && !binarySearch.enabled))
        0
      else
        BloomFilter.optimalSize(
          numberOfKeys = segmentUniqueKeysCount,
          falsePositiveRate = bloomFilter.falsePositiveRate,
          hasCompression = bloomFilter.hasCompression
        )

    val segmentSizeWithoutFooter: Int =
      segmentValuesSize +
        segmentSortedIndexSize +
        segmentHashIndexSize +
        segmentBinarySearchIndexSize +
        segmentBloomFilterSize

    //calculates the size of Segment after the last Group. This is used for size based grouping/compression.
    val segmentSizeWithoutFooterForNextGroup: Int =
      if (previous.exists(_.isGroup)) //if previous is a group, restart the size calculation
        segmentSizeWithoutFooter
      else //if previous is not a group, add previous key-values set segment size since the last group to this key-values Segment size.
        previousStats.map(_.segmentSizeWithoutFooterForNextGroup).getOrElse(0) +
          segmentSizeWithoutFooter

    val segmentFooterSize =
      Bytes.sizeOf(SegmentBlock.formatId) + //1 byte for format
        1 + //created in level
        1 + //hasGroup
        1 + //hasRange
        1 + //hasPut
        Bytes.sizeOf(chainPosition) + //key-values count
        Bytes.sizeOf(segmentUniqueKeysCount) +
        ByteSizeOf.long + //for CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
        Bytes.sizeOf(segmentValuesSize max 0) + //sorted index offset.
        Bytes.sizeOf(segmentSortedIndexSize) + //sorted index size.
        Bytes.sizeOf((segmentValuesSize + segmentSortedIndexSize) + 1) + //hash index offset. HashIndex offset will never be 0 since that's reserved for index is values are none..
        Bytes.sizeOf(segmentHashIndexSize) + //hash index size.
        Bytes.sizeOf((segmentValuesSize + segmentSortedIndexSize + segmentHashIndexSize + segmentBinarySearchIndexSize) + 1) + //binarySearch index
        Bytes.sizeOf(segmentBinarySearchIndexSize) + //binary index size.
        Bytes.sizeOf((segmentValuesSize + segmentSortedIndexSize + segmentHashIndexSize + segmentBinarySearchIndexSize + segmentBloomFilterSize) + 1) + //bloomFilter
        Bytes.sizeOf(segmentBloomFilterSize) + //bloomFilter
        ByteSizeOf.int //to store footer offset.

    val segmentSize: Int =
      segmentSizeWithoutFooter +
        segmentFooterSize

    val segmentUncompressedKeysSize: Int =
      previousStats.map(_.segmentUncompressedKeysSize).getOrElse(0) + keySize

    new Stats(
      valueSize = valueLength,
      segmentSize = segmentSize,
      chainPosition = chainPosition,
      segmentValueAndSortedIndexEntrySize = segmentValueAndSortedIndexEntrySize,
      segmentSortedIndexSizeWithoutHeader = segmentSortedIndexSizeWithoutHeader,
      groupsCount = groupsCount,
      segmentUniqueKeysCount = segmentUniqueKeysCount,
      segmentValuesSize = segmentValuesSize,
      segmentValuesSizeWithoutHeader = segmentValuesSizeWithoutHeader,
      segmentSortedIndexSize = segmentSortedIndexSize,
      segmentUncompressedKeysSize = segmentUncompressedKeysSize,
      segmentSizeWithoutFooter = segmentSizeWithoutFooter,
      segmentSizeWithoutFooterForNextGroup = segmentSizeWithoutFooterForNextGroup,
      segmentUniqueAccessIndexKeyCounts = segmentUniqueAccessIndexKeyCounts,
      keySize = indexEntry.size,
      thisKeyValuesSegmentKeyAndValueSize = thisKeyValuesSegmentKeyAndValueSize,
      thisKeyValuesIndexSizeWithoutFooterAndHeader = thisKeyValuesSortedIndexSizeWithoutFooterAndHeader,
      thisKeyValuesAccessIndexOffset = thisKeyValuesAccessIndexOffset,
      thisKeyValueIndexOffset = thisKeyValuesRealIndexOffset,
      segmentHashIndexSize = segmentHashIndexSize,
      segmentBloomFilterSize = segmentBloomFilterSize,
      segmentBinarySearchIndexSize = segmentBinarySearchIndexSize,
      segmentFooterSize = segmentFooterSize,
      segmentTotalNumberOfRanges = segmentTotalNumberOfRanges,
      segmentHasRemoveRange = hasRemoveRange,
      segmentHasRange = segmentHasRange,
      segmentHasPut = segmentHasPut,
      isGroup = isGroup
    )
  }
}

private[core] case class Stats(valueSize: Int,
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
                               keySize: Int,
                               thisKeyValuesSegmentKeyAndValueSize: Int,
                               thisKeyValuesIndexSizeWithoutFooterAndHeader: Int,
                               thisKeyValuesAccessIndexOffset: Int,
                               thisKeyValueIndexOffset: Int,
                               segmentHashIndexSize: Int,
                               segmentBloomFilterSize: Int,
                               segmentBinarySearchIndexSize: Int,
                               segmentFooterSize: Int,
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
    keySize + valueSize
}

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

package swaydb.core.data

import swaydb.core.util.{BloomFilterUtil, ByteUtilCore}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.concurrent.duration.Deadline
import swaydb.core.util.TimeUtil._

private[core] case class Stats(valueOffset: Int,
                               valueLength: Int,
                               segmentSize: Int,
                               commonBytes: Int,
                               position: Int,
                               keyWithoutCommonBytes: Slice[Byte],
                               segmentValuesSize: Int,
                               segmentUncompressedKeysSize: Int,
                               segmentSizeWithoutFooter: Int,
                               thisKeyValuesUncompressedKeySize: Int,
                               thisKeyValuesSegmentSizeWithoutFooter: Int,
                               thisKeyValuesIndexSizeWithoutFooter: Int,
                               hasRemoveRange: Boolean,
                               hasRange: Boolean) {
  def isNoneValue: Boolean =
    valueLength == 0

  def toValueOffset: Int =
    if (valueLength == 0)
      0
    else
      valueOffset + valueLength - 1

  def memorySegmentSize =
    segmentUncompressedKeysSize + segmentValuesSize

  def thisKeyValueMemorySize =
    thisKeyValuesUncompressedKeySize + valueLength

}
private[core] object Stats {

  def apply(key: Slice[Byte],
            falsePositiveRate: Double): Stats =
    Stats(
      key = key,
      valueLength = 0,
      valueOffset = 0,
      falsePositiveRate = falsePositiveRate,
      hasRemoveRange = false,
      hasRange = false,
      previous = None,
      deadlines = Seq.empty
    )

  def apply(key: Slice[Byte],
            falsePositiveRate: Double,
            deadlines: Iterable[Deadline]): Stats =
    Stats(
      key = key,
      valueLength = 0,
      valueOffset = 0,
      falsePositiveRate = falsePositiveRate,
      hasRemoveRange = false,
      hasRange = false,
      previous = None,
      deadlines = deadlines
    )

  def apply(key: Slice[Byte],
            falsePositiveRate: Double,
            previous: Option[KeyValue.WriteOnly],
            deadlines: Iterable[Deadline]): Stats =
    Stats(
      key = key,
      valueLength = 0,
      valueOffset = 0,
      falsePositiveRate = falsePositiveRate,
      hasRemoveRange = false,
      hasRange = false,
      previous = previous,
      deadlines = deadlines
    )

  def apply(key: Slice[Byte],
            value: Slice[Byte],
            falsePositiveRate: Double): Stats =
    Stats(
      key = key,
      valueLength = value.size,
      valueOffset = 0,
      falsePositiveRate = falsePositiveRate,
      hasRemoveRange = false,
      hasRange = false,
      previous = None,
      deadlines = Seq.empty
    )

  def apply(key: Slice[Byte],
            value: Slice[Byte],
            falsePositiveRate: Double,
            previous: Option[KeyValue.WriteOnly],
            deadlines: Iterable[Deadline]): Stats =
    Stats(
      key = key,
      value = Some(value),
      falsePositiveRate = falsePositiveRate,
      isRemoveRange = previous.exists(_.stats.hasRemoveRange),
      isRange = previous.exists(_.stats.hasRange),
      previous = previous,
      deadlines = deadlines
    )

  def apply(key: Slice[Byte],
            value: Option[Slice[Byte]],
            falsePositiveRate: Double,
            isRemoveRange: Boolean,
            isRange: Boolean,
            previous: Option[KeyValue.WriteOnly],
            deadlines: Iterable[Deadline]): Stats =
    Stats(
      key = key,
      valueLength = value.map(_.size).getOrElse(0),
      falsePositiveRate = falsePositiveRate,
      isRemoveRange = isRemoveRange,
      isRange = isRange,
      previous = previous,
      deadlines = deadlines
    )

  def apply(key: Slice[Byte],
            valueLength: Int,
            falsePositiveRate: Double,
            isRemoveRange: Boolean,
            isRange: Boolean,
            previous: Option[KeyValue.WriteOnly],
            deadlines: Iterable[Deadline]): Stats = {
    val valueOffset =
      previous match {
        case Some(previous) =>
          if (previous.stats.valueLength > 0)
            previous.stats.toValueOffset + 1
          else
          //if previous key-values's value is None, keep the valueOffset of the previous key-value's
          //so that next key-value can continue from that offset.
            previous.stats.valueOffset
        case _ =>
          0
      }

    Stats(
      key = key,
      valueLength = valueLength,
      valueOffset = valueOffset,
      falsePositiveRate = falsePositiveRate,
      hasRemoveRange = isRemoveRange || previous.exists(_.stats.hasRemoveRange),
      hasRange = isRemoveRange || isRange || previous.exists(_.stats.hasRange),
      previous = previous,
      deadlines = deadlines
    )
  }

  private def apply(key: Slice[Byte],
                    valueLength: Int,
                    valueOffset: Int,
                    falsePositiveRate: Double,
                    hasRemoveRange: Boolean,
                    hasRange: Boolean,
                    previous: Option[KeyValue.WriteOnly],
                    deadlines: Iterable[Deadline]): Stats = {

    val commonBytes: Int =
      previous.map {
        case previousKeyValue: KeyValue.WriteOnly.Range =>
          ByteUtilCore.commonPrefixBytes(previousKeyValue.fullKey, key)

        case previousKeyValue =>
          ByteUtilCore.commonPrefixBytes(previousKeyValue.key, key)
      } getOrElse 0

    val keyWithoutCommonBytes =
      if (commonBytes != 0)
        key.slice(commonBytes, key.size - 1)
      else
        key

    val keyLengthWithoutCommonBytes =
      keyWithoutCommonBytes.size

    val previousStats =
      previous.map(_.stats)

    val position =
      previousStats.map(_.position + 1) getOrElse 1

    val thisKeyValuesIndexSizeWithoutFooter =
      if (valueLength == 0) {
        val indexSize =
          1 +
            ByteUtilCore.sizeUnsignedInt(commonBytes) +
            ByteUtilCore.sizeUnsignedInt(keyLengthWithoutCommonBytes) +
            keyLengthWithoutCommonBytes +
            ByteUtilCore.sizeUnsignedInt(0) + {
            if (deadlines.isEmpty)
              1
            else
              deadlines.foldLeft(0)((count, deadline) => count + ByteUtilCore.sizeUnsignedLong(deadline.toNanos))
          }

        ByteUtilCore.sizeUnsignedInt(indexSize) + indexSize
      } else {
        val indexSize =
          1 +
            ByteUtilCore.sizeUnsignedInt(commonBytes) +
            ByteUtilCore.sizeUnsignedInt(keyLengthWithoutCommonBytes) +
            keyLengthWithoutCommonBytes +
            ByteUtilCore.sizeUnsignedInt(valueLength) +
            ByteUtilCore.sizeUnsignedInt(valueOffset) + {
            if (deadlines.isEmpty)
              1
            else
              deadlines.foldLeft(0)((count, deadline) => count + ByteUtilCore.sizeUnsignedLong(deadline.toNanos))
          }

        ByteUtilCore.sizeUnsignedInt(indexSize) + indexSize
      }

    val thisKeyValuesSegmentSizeWithoutFooter: Int =
      thisKeyValuesIndexSizeWithoutFooter + valueLength

    val segmentSizeWithoutFooter: Int =
      previousStats.map(_.segmentSizeWithoutFooter).getOrElse(0) + thisKeyValuesSegmentSizeWithoutFooter

    val segmentValuesSize: Int =
      previousStats.map(_.segmentValuesSize).getOrElse(0) + valueLength

    val footerSize =
      1 + //1 byte for format
        1 + //for hasRange
        ByteSizeOf.long + //for CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
        ByteUtilCore.sizeUnsignedInt(segmentValuesSize) + //index offset
        ByteUtilCore.sizeUnsignedInt(position) + //key-values count
        {
          if (hasRemoveRange) { //BloomFilter is not created when hasRemoveRange is true.
            1 //(1 for unsigned int 0)
          } else {
            val bloomFilterByteSize = BloomFilterUtil.byteSize(position, falsePositiveRate)
            ByteUtilCore.sizeUnsignedInt(bloomFilterByteSize) + bloomFilterByteSize
          }
        }

    val segmentSize: Int =
      segmentSizeWithoutFooter + footerSize + ByteSizeOf.int

    val segmentUncompressedKeysSize: Int =
      previousStats.map(_.segmentUncompressedKeysSize).getOrElse(0) + key.size

    new Stats(
      valueOffset = valueOffset,
      valueLength = valueLength,
      segmentSize = segmentSize,
      commonBytes = commonBytes,
      position = position,
      keyWithoutCommonBytes = keyWithoutCommonBytes,
      segmentValuesSize = segmentValuesSize,
      segmentUncompressedKeysSize = segmentUncompressedKeysSize,
      segmentSizeWithoutFooter = segmentSizeWithoutFooter,
      thisKeyValuesUncompressedKeySize = key.size,
      thisKeyValuesSegmentSizeWithoutFooter = thisKeyValuesSegmentSizeWithoutFooter,
      thisKeyValuesIndexSizeWithoutFooter = thisKeyValuesIndexSizeWithoutFooter,
      hasRemoveRange = hasRemoveRange,
      hasRange = hasRange
    )
  }
}

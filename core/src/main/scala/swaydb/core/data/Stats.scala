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

import swaydb.core.data.Persistent.{Put, Removed}
import swaydb.core.util.{BloomFilterUtil, ByteUtilCore}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

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
                               thisKeyValuesIndexSizeWithoutFooter: Int) {
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
            isDelete: Boolean,
            falsePositiveRate: Double): Stats =
    Stats(key, 0, 0, isDelete, falsePositiveRate, None)

  def apply(key: Slice[Byte],
            value: Slice[Byte],
            falsePositiveRate: Double,
            isDelete: Boolean): Stats =
    Stats(key, value.size, 0, isDelete, falsePositiveRate, None)

  def apply(key: Slice[Byte],
            value: Slice[Byte],
            isDelete: Boolean,
            falsePositiveRate: Double,
            previousStats: Option[KeyValue]): Stats =
    Stats(key, Some(value), isDelete, falsePositiveRate, previousStats)

  def apply(key: Slice[Byte],
            value: Option[Slice[Byte]],
            isDelete: Boolean,
            falsePositiveRate: Double,
            previous: Option[KeyValue]): Stats =
    Stats(key, value.map(_.size).getOrElse(0), isDelete, falsePositiveRate, previous)

  def apply(key: Slice[Byte],
            valueLength: Int,
            isDelete: Boolean,
            falsePositiveRate: Double,
            previous: Option[KeyValue]): Stats = {
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
    Stats(key, valueLength, valueOffset, isDelete, falsePositiveRate, previous)
  }

  private def apply(key: Slice[Byte],
                    valueLength: Int,
                    valueOffset: Int,
                    isDelete: Boolean,
                    falsePositiveRate: Double,
                    previous: Option[KeyValue]): Stats = {

    val commonBytes: Int =
      previous.map(previousKeyValue => ByteUtilCore.commonPrefixBytes(previousKeyValue.key, key)) getOrElse 0

    val keyWithoutCommonBytes =
      if (commonBytes != 0)
        key.slice(commonBytes, key.size - 1)
      else
        key

    val keyLength =
      keyWithoutCommonBytes.size

    val previousStats =
      previous.map(_.stats)

    val position =
      previousStats.map(_.position + 1) getOrElse 1

    val thisKeyValuesIndexSizeWithoutFooter =
      if (isDelete) {
        val indexSize =
          ByteUtilCore.sizeUnsignedInt(Removed.id) +
            ByteUtilCore.sizeUnsignedInt(commonBytes) +
            ByteUtilCore.sizeUnsignedInt(keyLength) +
            keyLength

        ByteUtilCore.sizeUnsignedInt(indexSize) + indexSize
      }
      else if (valueLength == 0) {
        val indexSize =
          ByteUtilCore.sizeUnsignedInt(Put.id) +
            ByteUtilCore.sizeUnsignedInt(commonBytes) +
            ByteUtilCore.sizeUnsignedInt(keyLength) +
            keyLength +
            ByteUtilCore.sizeUnsignedInt(0)

        ByteUtilCore.sizeUnsignedInt(indexSize) + indexSize
      }
      else {
        val indexSize =
          ByteUtilCore.sizeUnsignedInt(Put.id) +
            ByteUtilCore.sizeUnsignedInt(commonBytes) +
            ByteUtilCore.sizeUnsignedInt(keyLength) +
            keyLength +
            ByteUtilCore.sizeUnsignedInt(valueLength) +
            ByteUtilCore.sizeUnsignedInt(valueOffset)

        ByteUtilCore.sizeUnsignedInt(indexSize) + indexSize
      }

    val thisKeyValuesSegmentSizeWithoutFooter: Int =
      thisKeyValuesIndexSizeWithoutFooter + valueLength

    val segmentSizeWithoutFooter: Int =
      previousStats.map(_.segmentSizeWithoutFooter).getOrElse(0) + thisKeyValuesSegmentSizeWithoutFooter

    val segmentValuesSize: Int =
      previousStats.map(_.segmentValuesSize).getOrElse(0) + valueLength

    val footerSize =
      ByteSizeOf.long + //for CRC. This cannot be unsignedLong because the size of the crc long bytes is not fixed.
        ByteUtilCore.sizeUnsignedInt(segmentValuesSize) + //index offset
        ByteUtilCore.sizeUnsignedInt(position) + //key-values count
        BloomFilterUtil.byteSize(position, falsePositiveRate) +
        1 //1 byte for format

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
      thisKeyValuesIndexSizeWithoutFooter = thisKeyValuesIndexSizeWithoutFooter
    )
  }
}

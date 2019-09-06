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

package swaydb.core.segment.format.a.block

import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.{Error, IO}

private[block] sealed trait BinarySearchContext {
  val bytesPerValue: Int
  val valuesCount: Int
  val isFullIndex: Boolean
  val higherOrLower: Option[Boolean]
  val startKeyValue: Option[Persistent]
  val endKeyValue: Option[Persistent]
  def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result]
}

private[block] object BinarySearchContext {
  def apply(key: Slice[Byte],
            highOrLow: Option[Boolean],
            start: Option[Persistent],
            end: Option[Persistent],
            binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher =
        highOrLow map {
          higher =>
            //if the sortedIndex has compression disabled do not fetch the next key-value. Let binary search find the next one to seek to.
            if (higher)
              if (sortedIndex.block.hasPrefixCompression)
                KeyMatcher.Higher.WhilePrefixCompressed(key)
              else
                KeyMatcher.Higher.SeekOne(key)
            else if (sortedIndex.block.hasPrefixCompression)
              KeyMatcher.Lower.WhilePrefixCompressed(key)
            else
              KeyMatcher.Lower.SeekOne(key)
        } getOrElse {
          if (sortedIndex.block.hasPrefixCompression)
            KeyMatcher.Get.WhilePrefixCompressed(key)
          else
            KeyMatcher.Get.SeekOne(key)
        }

      override val bytesPerValue: Int = binarySearchIndex.block.bytesPerValue

      override val isFullIndex: Boolean = binarySearchIndex.block.isFullIndex

      override val valuesCount: Int = binarySearchIndex.block.valuesCount

      override val higherOrLower: Option[Boolean] = highOrLow

      override val startKeyValue: Option[Persistent] = start

      override val endKeyValue: Option[Persistent] = end

      override def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] =
        binarySearchIndex
          .moveTo(offset)
          .readInt(unsigned = binarySearchIndex.block.isVarInt)
          .flatMap {
            sortedIndexOffsetValue =>
              SortedIndexBlock.findAndMatchOrNextMatch(
                matcher = matcher,
                fromOffset = sortedIndexOffsetValue,
                sortedIndex = sortedIndex,
                valuesReader = values
              )
          }
    }

  def apply(key: Slice[Byte],
            highOrLow: Option[Boolean],
            start: Option[Persistent],
            end: Option[Persistent],
            keyValuesCount: Int,
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher =
        highOrLow map {
          higher =>
            //if the sortedIndex has compression disabled do not fetch the next key-value. Let binary search find the next one to seek to.
            if (higher)
              KeyMatcher.Higher.SeekOne(key)
            else
              KeyMatcher.Lower.SeekOne(key)
        } getOrElse {
          KeyMatcher.Get.SeekOne(key)
        }

      override val bytesPerValue: Int = sortedIndex.block.segmentMaxIndexEntrySize

      override val isFullIndex: Boolean = true

      override val valuesCount: Int = keyValuesCount

      override val higherOrLower: Option[Boolean] = highOrLow

      override val startKeyValue: Option[Persistent] = start

      override val endKeyValue: Option[Persistent] = end

      override def seek(offset: Int): IO[Error.Segment, KeyMatcher.Result] =
        SortedIndexBlock.findAndMatchOrNextMatch(
          matcher = matcher,
          fromOffset = offset,
          sortedIndex = sortedIndex,
          valuesReader = values
        )
    }
}
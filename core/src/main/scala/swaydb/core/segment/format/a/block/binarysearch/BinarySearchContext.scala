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

package swaydb.core.segment.format.a.block.binarysearch

import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.segment.format.a.block.KeyMatcher.Get
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{KeyMatcher, SortedIndexBlock, ValuesBlock}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[block] trait BinarySearchContext {
  def targetKey: Slice[Byte]
  def bytesPerValue: Int
  def valuesCount: Int
  def isFullIndex: Boolean
  def lowestKeyValue: PersistentOptional
  def highestKeyValue: PersistentOptional

  def seek(offset: Int): KeyMatcher.Result
}

object BinarySearchContext {

  def apply(key: Slice[Byte],
            lowest: PersistentOptional,
            highest: PersistentOptional,
            binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher: Get.MatchOnly = KeyMatcher.Get.MatchOnly(key)

      override val targetKey = key

      override val bytesPerValue: Int = binarySearchIndex.block.bytesPerValue

      override val isFullIndex: Boolean = binarySearchIndex.block.isFullIndex

      override val valuesCount: Int = binarySearchIndex.block.valuesCount

      override val lowestKeyValue: PersistentOptional = lowest

      override val highestKeyValue: PersistentOptional = highest

      override def seek(offset: Int): KeyMatcher.Result = {
        val partialKeyValue =
          binarySearchIndex.block.format.read(
            offset = offset,
            seekSize = binarySearchIndex.block.bytesPerValue,
            binarySearchIndex = binarySearchIndex,
            sortedIndex = sortedIndex,
            values = values
          )
        //todo - hasMore should be calculated.
        matcher(previous = partialKeyValue, next = Persistent.Partial.Null, hasMore = true)
      }
    }

  def apply(key: Slice[Byte],
            lowest: PersistentOptional,
            highest: PersistentOptional,
            keyValuesCount: Int,
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher: Get.MatchOnly = KeyMatcher.Get.MatchOnly(key)

      override val targetKey = key

      override val bytesPerValue: Int = sortedIndex.block.segmentMaxIndexEntrySize

      override val isFullIndex: Boolean = true

      override val valuesCount: Int = keyValuesCount

      override val lowestKeyValue: PersistentOptional = lowest

      override val highestKeyValue: PersistentOptional = highest

      override def seek(offset: Int): KeyMatcher.Result = {
        val partialKeyValue =
          SortedIndexBlock.readPartial(
            fromOffset = offset,
            sortedIndexReader = sortedIndex,
            valuesReader = values
          )

        //todo - hasMore should be calculated.
        matcher(previous = partialKeyValue, next = Persistent.Partial.Null, hasMore = true)
      }
    }
}

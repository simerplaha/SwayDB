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

  def seekAndMatchMutate(offset: Int): Persistent.Partial
}

object BinarySearchContext {

  def apply(key: Slice[Byte],
            lowest: PersistentOptional,
            highest: PersistentOptional,
            binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            valuesNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {

      override def targetKey = key

      override def bytesPerValue: Int = binarySearchIndex.block.bytesPerValue

      override def isFullIndex: Boolean = binarySearchIndex.block.isFullIndex

      override def valuesCount: Int = binarySearchIndex.block.valuesCount

      override def lowestKeyValue: PersistentOptional = lowest

      override def highestKeyValue: PersistentOptional = highest

      override def seekAndMatchMutate(offset: Int): Persistent.Partial =
        binarySearchIndex.block.format.read(
          offset = offset,
          seekSize = binarySearchIndex.block.bytesPerValue,
          binarySearchIndex = binarySearchIndex,
          sortedIndex = sortedIndex,
          valuesNullable = valuesNullable
        ).matchMutateForBinarySearch(key)
    }

  def apply(key: Slice[Byte],
            lowest: PersistentOptional,
            highest: PersistentOptional,
            keyValuesCount: Int,
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            valuesNullable: UnblockedReader[ValuesBlock.Offset, ValuesBlock])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {

      override def targetKey = key

      override def bytesPerValue: Int = sortedIndex.block.segmentMaxIndexEntrySize

      override def isFullIndex: Boolean = true

      override def valuesCount: Int = keyValuesCount

      override def lowestKeyValue: PersistentOptional = lowest

      override def highestKeyValue: PersistentOptional = highest

      override def seekAndMatchMutate(offset: Int): Persistent.Partial =
        SortedIndexBlock.readPartialKeyValue(
          fromOffset = offset,
          sortedIndexReader = sortedIndex,
          valuesReaderNullable = valuesNullable
        ).matchMutateForBinarySearch(key)
    }
}

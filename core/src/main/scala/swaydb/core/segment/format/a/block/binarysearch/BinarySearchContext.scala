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

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.KeyMatcher.Get
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{KeyMatcher, SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

private[block] trait BinarySearchContext {
  def targetKey: Slice[Byte]
  def bytesPerValue: Int
  def valuesCount: Int
  def isFullIndex: Boolean
  def lowestKeyValue: Option[Persistent]
  def highestKeyValue: Option[Persistent]

  def seek(offset: Int): KeyMatcher.Result
}

object BinarySearchContext {

  def apply(key: Slice[Byte],
            lowest: Option[Persistent],
            highest: Option[Persistent],
            binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher: Get.MatchOnly = KeyMatcher.Get.MatchOnly(key)

      override val targetKey = key

      override val bytesPerValue: Int = binarySearchIndex.block.bytesPerValue

      override val isFullIndex: Boolean = binarySearchIndex.block.isFullIndex

      override val valuesCount: Int = binarySearchIndex.block.valuesCount

      override val lowestKeyValue: Option[Persistent] = lowest

      override val highestKeyValue: Option[Persistent] = highest

      override def seek(offset: Int): KeyMatcher.Result = {
        //read entry information at the given offset
        val binaryEntryReader = Reader(binarySearchIndex.moveTo(offset).read(binarySearchIndex.block.bytesPerValue))
        val keyOffset = binaryEntryReader.readUnsignedInt()
        val keySize = binaryEntryReader.readUnsignedInt()
        val keyType = binaryEntryReader.get()

        //read the target key at the offset within sortedIndex
        val entryKey = sortedIndex.moveTo(keyOffset).read(keySize)

        //create a temporary partially read key-value for matcher.
        val partialKeyValue =
          if (keyType == Transient.Range.id)
            new Partial.Range {
              val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

              override lazy val indexOffset: Int =
                binaryEntryReader.readUnsignedInt()

              override def key: Slice[Byte] =
                fromKey

              override def toPersistent: Persistent =
                SortedIndexBlock.read(
                  fromOffset = indexOffset,
                  overwriteNextIndexOffset = None,
                  sortedIndexReader = sortedIndex,
                  valuesReader = values
                )
            }
          else if (keyType == Transient.Put.id || keyType == Transient.Remove.id || keyType == Transient.Update.id || keyType == Transient.Function.id || keyType == Transient.PendingApply.id)
            new Partial.Fixed {
              override lazy val indexOffset: Int =
                binaryEntryReader.readUnsignedInt()

              override def key: Slice[Byte] =
                entryKey

              override def toPersistent: Persistent =
                SortedIndexBlock.read(
                  fromOffset = indexOffset,
                  overwriteNextIndexOffset = None,
                  sortedIndexReader = sortedIndex,
                  valuesReader = values
                )
            }
          else
            throw new Exception(s"Invalid keyType: $keyType, offset: $offset, keyOffset: $keyOffset, keySize: $keySize")

        //todo - hasMore should be calculated.
        matcher(previous = partialKeyValue, next = None, hasMore = true)
      }
    }

  def apply(key: Slice[Byte],
            lowest: Option[Persistent],
            highest: Option[Persistent],
            keyValuesCount: Int,
            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
            values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): BinarySearchContext =
    new BinarySearchContext {
      val matcher: Get.MatchOnly = KeyMatcher.Get.MatchOnly(key)

      override val targetKey = key

      override val bytesPerValue: Int = sortedIndex.block.segmentMaxIndexEntrySize

      override val isFullIndex: Boolean = true

      override val valuesCount: Int = keyValuesCount

      override val lowestKeyValue: Option[Persistent] = lowest

      override val highestKeyValue: Option[Persistent] = highest

      override def seek(offset: Int): KeyMatcher.Result =
        if (sortedIndex.block.isPreNormalised) {
          //if sortedIndex was pre-normalised then there will be not empty bytes to skip so simply read the key-values.
          SortedIndexBlock.readAndMatch(
            matcher = matcher,
            fromOffset = offset,
            overwriteNextIndexOffset = None,
            sortedIndexReader = sortedIndex,
            valuesReader = values
          )
        } else {
          //read enough information from the indexEntry to perform match.
          val indexSize = sortedIndex.moveTo(offset).readUnsignedInt()
          val indexEntryReader = Reader(sortedIndex.read(indexSize))
          if (sortedIndex.block.enableAccessPositionIndex) indexEntryReader.readUnsignedInt()
          val keySize = indexEntryReader.readUnsignedInt()
          val entryKey = indexEntryReader.read(keySize)
          val keyValueId = indexEntryReader.readUnsignedInt()

          //create a temporary partially read key-value for matcher.
          val partialKeyValue =
            if (KeyValueId.Range hasKeyValueId keyValueId)
              new Partial.Range {
                val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

                override def indexOffset: Int =
                  offset

                override def key: Slice[Byte] =
                  fromKey

                override def toPersistent: Persistent =
                  SortedIndexBlock.read(
                    fromOffset = offset,
                    overwriteNextIndexOffset = None,
                    sortedIndexReader = sortedIndex,
                    valuesReader = values
                  )
              }
            else if (KeyValueId isFixedId keyValueId)
              new Partial.Fixed {
                override def indexOffset: Int =
                  offset

                override def key: Slice[Byte] =
                  entryKey

                override def toPersistent: Persistent =
                  SortedIndexBlock.read(
                    fromOffset = offset,
                    overwriteNextIndexOffset = None,
                    sortedIndexReader = sortedIndex,
                    valuesReader = values
                  )
              }
            else
              throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, indexSize: $indexSize")

          val hasMore = (offset + indexSize) < sortedIndex.block.offset.end

          matcher(previous = partialKeyValue, next = None, hasMore = hasMore)
        }
    }
}

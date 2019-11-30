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
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.macros.SealedList

sealed trait BinarySearchEntryFormat {
  def id: Byte

  def isCopy: Boolean

  def isReference: Boolean = !isCopy

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestKeyOffset: Int,
                              largestKeySize: Int): Int

  def write(indexOffset: Int,
            keyOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            bytes: Slice[Byte]): Unit

  def read(offset: Int,
           seekSize: Int,
           searchIndex: UnblockedReader[_, _],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial
}

object BinarySearchEntryFormat {

  val formats: List[BinarySearchEntryFormat] = SealedList.list[BinarySearchEntryFormat]

  object ReferenceIndex extends BinarySearchEntryFormat {
    //ids start from 1 instead of 0 to account for entries that don't allow zero bytes.
    override val id: Byte = 0.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int =
      Bytes sizeOfUnsignedInt largestIndexOffset

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit =
      bytes addUnsignedInt indexOffset

    override def read(offset: Int,
                      seekSize: Int,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val sortedIndexOffsetValue =
        searchIndex
          .moveTo(offset)
          .readUnsignedInt()

      SortedIndexBlock.read(
        fromOffset = sortedIndexOffsetValue,
        overwriteNextIndexOffset = None,
        sortedIndexReader = sortedIndex,
        valuesReader = values
      )
    }
  }

  object ReferenceKey extends BinarySearchEntryFormat {
    override val id: Byte = 1.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {

      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeyOffset = Bytes.sizeOfUnsignedInt(largestKeyOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize)

      sizeOfLargestKeyOffset + sizeOfLargestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit = {
      bytes addUnsignedInt keyOffset
      bytes addUnsignedInt mergedKey.size
      bytes add keyType
      bytes addUnsignedInt indexOffset
    }

    override def read(offset: Int,
                      seekSize: Int,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val searchIndexReader = Reader(searchIndex.moveTo(offset).read(seekSize))

      val keyOffset = searchIndexReader.readUnsignedInt()
      val keySize = searchIndexReader.readUnsignedInt()
      val keyType = searchIndexReader.get()

      //read the target key at the offset within sortedIndex
      val entryKey = sortedIndex.moveTo(keyOffset).read(keySize)

      //create a temporary partially read key-value for matcher.
      if (keyType == Transient.Range.id)
        new Partial.Range {
          val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

          override lazy val indexOffset: Int =
            searchIndexReader.readUnsignedInt()

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
            searchIndexReader.readUnsignedInt()

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
    }
  }

  object CopyKey extends BinarySearchEntryFormat {
    override val id: Byte = 2.toByte

    override def isCopy: Boolean = true

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize)
      sizeOfLargestKeySize + largestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit = {
      bytes addUnsignedInt mergedKey.size
      bytes addAll mergedKey
      bytes add keyType
      bytes addUnsignedInt indexOffset
    }

    override def read(offset: Int,
                      seekSize: Int,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val binaryEntryReader = Reader(searchIndex.moveTo(offset).read(seekSize))
      val keySize = binaryEntryReader.readUnsignedInt()
      val entryKey = binaryEntryReader.read(keySize)
      val keyType = binaryEntryReader.get()

      //create a temporary partially read key-value for matcher.
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
        throw new Exception(s"Invalid keyType: $keyType, offset: $offset, keySize: $keySize, keyType: $keyType")
    }
  }
}

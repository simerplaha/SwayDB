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
import swaydb.core.segment.format.a.entry.id.KeyValueId
import swaydb.core.util.Bytes
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.macros.SealedList

sealed trait BinarySearchIndexSerialiser {
  def id: Byte

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestKeyOffset: Int,
                              largestKeySize: Int): Int

  def write(indexOffset: Int,
            keyOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            bytes: Slice[Byte]): Unit

  def read(offset: Int,
           binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial
}

object BinarySearchIndexSerialiser {

  val formats: List[BinarySearchIndexSerialiser] = SealedList.list[BinarySearchIndexSerialiser]

  def apply(id: Int): Option[BinarySearchIndexSerialiser] =
    formats.find(_.id == id)

  object CopyKey extends BinarySearchIndexSerialiser {
    override val id: Byte = 0.toByte

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
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val binaryEntryReader = Reader(binarySearchIndex.moveTo(offset).read(binarySearchIndex.block.bytesPerValue))
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

  object ReferenceKey extends BinarySearchIndexSerialiser {
    override val id: Byte = 1.toByte

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
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val binaryEntryReader = Reader(binarySearchIndex.moveTo(offset).read(binarySearchIndex.block.bytesPerValue))
      val keyOffset = binaryEntryReader.readUnsignedInt()
      val keySize = binaryEntryReader.readUnsignedInt()
      val keyType = binaryEntryReader.get()

      //read the target key at the offset within sortedIndex
      val entryKey = sortedIndex.moveTo(keyOffset).read(keySize)

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
        throw new Exception(s"Invalid keyType: $keyType, offset: $offset, keyOffset: $keyOffset, keySize: $keySize")
    }
  }

  object ReferenceIndex extends BinarySearchIndexSerialiser {
    override val id: Byte = 2.toByte

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
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val sortedIndexOffsetValue =
        binarySearchIndex
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

  /**
   * Parses [[BinarySearchIndexBlock]]'s entry into [[Persistent.Partial]] at the given offset when the bytes are normalised.
   */
  def readNormalised(offset: Int,
                     sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                     values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]])(implicit ordering: KeyOrder[Slice[Byte]]): (Persistent.Partial, Int) = {
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

    (partialKeyValue, indexSize)
  }
}

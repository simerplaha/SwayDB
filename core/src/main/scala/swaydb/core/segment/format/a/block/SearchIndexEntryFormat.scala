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

import swaydb.IO
import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.macros.SealedList

sealed trait SearchIndexEntryFormat {
  def id: Byte

  def bytesToAllocatePerEntry(nonZero: Boolean,
                              largestIndexOffset: Int,
                              largestKeyOffset: Int,
                              largestKeySize: Int): Int

  def write(indexOffset: Int,
            keyOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            nonZero: Boolean,
            bytes: Slice[Byte]): Unit

  def read(offset: Int,
           seekSize: Int,
           nonZero: Boolean,
           searchIndex: UnblockedReader[_, _],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial
}

object SearchIndexEntryFormat {

  val formats: List[SearchIndexEntryFormat] = SealedList.list[SearchIndexEntryFormat]

  def apply(id: Int): Option[SearchIndexEntryFormat] =
    formats.find(_.id == id)

  object ReferenceIndex extends SearchIndexEntryFormat {
    override val id: Byte = 0.toByte

    override def bytesToAllocatePerEntry(nonZero: Boolean,
                                         largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int =
      if (nonZero)
        Bytes sizeOfUnsignedInt (largestIndexOffset + 1)
      else
        Bytes sizeOfUnsignedInt largestIndexOffset

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       nonZero: Boolean,
                       bytes: Slice[Byte]): Unit =
      if (nonZero)
        bytes addUnsignedInt (indexOffset + 1)
      else
        bytes addUnsignedInt indexOffset

    override def read(offset: Int,
                      seekSize: Int,
                      nonZero: Boolean,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val sortedIndexOffsetValue =
        searchIndex
          .moveTo(offset)
          .readUnsignedInt()

      SortedIndexBlock.read(
        fromOffset = if (nonZero) sortedIndexOffsetValue - 1 else sortedIndexOffsetValue,
        overwriteNextIndexOffset = None,
        sortedIndexReader = sortedIndex,
        valuesReader = values
      )
    }
  }

  object ReferenceKey extends SearchIndexEntryFormat {
    override val id: Byte = 1.toByte

    override def bytesToAllocatePerEntry(nonZero: Boolean,
                                         largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {

      val (_largestIndexOffset, _largestKeyOffset, _largestKeySize) =
        if (nonZero) {
          (largestIndexOffset + 1, largestKeyOffset + 1, largestKeySize + 1)
        } else {
          (largestIndexOffset, largestKeyOffset, largestKeySize)
        }

      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(_largestIndexOffset)
      val sizeOfLargestKeyOffset = Bytes.sizeOfUnsignedInt(_largestKeyOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(_largestKeySize)

      sizeOfLargestKeyOffset + sizeOfLargestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       nonZero: Boolean,
                       bytes: Slice[Byte]): Unit =
      if (nonZero) {
        bytes addUnsignedInt (keyOffset + 1)
        bytes addUnsignedInt (mergedKey.size + 1)
        bytes add (keyType + 1).toByte
        bytes addUnsignedInt (indexOffset + 1)
      }
      else {
        bytes addUnsignedInt keyOffset
        bytes addUnsignedInt mergedKey.size
        bytes add keyType
        bytes addUnsignedInt indexOffset
      }

    override def read(offset: Int,
                      seekSize: Int,
                      nonZero: Boolean,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {
      val searchIndexReader = Reader(searchIndex.moveTo(offset).read(seekSize))

      val _keyOffset = searchIndexReader.readUnsignedInt()
      val _keySize = searchIndexReader.readUnsignedInt()
      val _keyType = searchIndexReader.get()

      val (keyOffset, keySize, keyType) =
        if (nonZero) {
          (_keyOffset - 1, _keySize - 1, _keyType - 1)
        } else {
          (_keyOffset, _keySize, _keyType)
        }

      //read the target key at the offset within sortedIndex
      val entryKey = sortedIndex.moveTo(keyOffset).read(keySize)

      //create a temporary partially read key-value for matcher.
      if (keyType == Transient.Range.id)
        new Partial.Range {
          val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

          override lazy val indexOffset: Int =
            if (nonZero)
              searchIndexReader.readUnsignedInt() - 1
            else
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
            if (nonZero)
              searchIndexReader.readUnsignedInt() - 1
            else
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

  object CopyKey extends SearchIndexEntryFormat {
    override val id: Byte = 2.toByte

    override def bytesToAllocatePerEntry(nonZero: Boolean,
                                         largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int =
      if (nonZero) {
        IO.throws(this.getClass.getSimpleName + " cannot write nonZero entry")
      } else {
        val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
        val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize)
        sizeOfLargestKeySize + largestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
      }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       nonZero: Boolean,
                       bytes: Slice[Byte]): Unit =
      if (nonZero) {
        IO.throws(this.getClass.getSimpleName + " cannot write nonZero entry")
      } else {
        bytes addUnsignedInt mergedKey.size
        bytes addAll mergedKey
        bytes add keyType
        bytes addUnsignedInt indexOffset
      }

    override def read(offset: Int,
                      seekSize: Int,
                      nonZero: Boolean,
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial =
      if (nonZero) {
        IO.throws(this.getClass.getSimpleName + " does not create nonZero entry.")
      } else {
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

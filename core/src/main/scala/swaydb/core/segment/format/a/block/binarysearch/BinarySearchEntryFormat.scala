/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.binarysearch

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.Bytes
import swaydb.data.config.IndexFormat
import swaydb.data.slice.Slice._
import swaydb.data.util.ByteSizeOf
import swaydb.macros.Sealed
import swaydb.data.util.ByteOps._

private[core] sealed trait BinarySearchEntryFormat {
  def id: Byte

  def isCopy: Boolean

  def isReference: Boolean = !isCopy

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestMergedKeySize: Int): Int

  def write(indexOffset: Int,
            mergedKey: Sliced[Byte],
            keyType: Byte,
            bytes: Sliced[Byte]): Unit

  def read(offset: Int,
           seekSize: Int,
           binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial
}

private[core] object BinarySearchEntryFormat {

  def apply(indexFormat: IndexFormat): BinarySearchEntryFormat =
    indexFormat match {
      case IndexFormat.Reference =>
        BinarySearchEntryFormat.Reference

      case IndexFormat.CopyKey =>
        BinarySearchEntryFormat.CopyKey
    }

  object Reference extends BinarySearchEntryFormat {
    //ids start from 1 instead of 0 to account for entries that don't allow zero bytes.
    override val id: Byte = 0.toByte

    override val isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestMergedKeySize: Int): Int =
      Bytes sizeOfUnsignedInt largestIndexOffset

    override def write(indexOffset: Int,
                       mergedKey: Sliced[Byte],
                       keyType: Byte,
                       bytes: Sliced[Byte]): Unit =
      bytes addUnsignedInt indexOffset

    override def read(offset: Int,
                      seekSize: Int,
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
      val sortedIndexOffsetValue =
        binarySearchIndex
          .moveTo(offset)
          .readUnsignedInt()

      SortedIndexBlock.readPartialKeyValue(
        fromOffset = sortedIndexOffsetValue,
        sortedIndexReader = sortedIndex,
        valuesReaderOrNull = valuesOrNull
      )
    }
  }

  object CopyKey extends BinarySearchEntryFormat {
    override val id: Byte = 2.toByte

    override val isCopy: Boolean = true

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestMergedKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestMergedKeySize)
      sizeOfLargestKeySize + largestMergedKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       mergedKey: Sliced[Byte],
                       keyType: Byte,
                       bytes: Sliced[Byte]): Unit = {
      bytes addUnsignedInt mergedKey.size
      bytes addAll mergedKey
      bytes add keyType
      bytes addUnsignedInt indexOffset
    }

    override def read(offset: Int,
                      seekSize: Int,
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlock.Offset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
      val entryReader = Reader(binarySearchIndex.moveTo(offset).read(seekSize))
      val keySize = entryReader.readUnsignedInt()
      val entryKey = entryReader.read(keySize)
      val keyType = entryReader.get()
      var entryIndexOffsetPlaceholder: Int = -2
      var readPersistentValue: Persistent = null

      def parseIndexOffset = {
        if (entryIndexOffsetPlaceholder == -2)
          entryIndexOffsetPlaceholder = entryReader.readUnsignedInt()
        entryIndexOffsetPlaceholder
      }

      def parsePersistent: Persistent = {
        if (readPersistentValue == null)
          readPersistentValue =
            SortedIndexBlock.read(
              fromOffset = parseIndexOffset,
              keySize = keySize,
              sortedIndexReader = sortedIndex,
              valuesReaderOrNull = valuesOrNull
            )

        readPersistentValue
      }


      //create a temporary partially read key-value for matcher.
      if (keyType == Memory.Range.id)
        new Partial.Range {
          val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

          override def indexOffset: Int =
            parseIndexOffset

          override def key: Sliced[Byte] =
            fromKey

          override def toPersistent: Persistent =
            parsePersistent
        }
      else if (keyType == Memory.Put.id || keyType == Memory.Remove.id || keyType == Memory.Update.id || keyType == Memory.Function.id || keyType == Memory.PendingApply.id)
        new Partial.Fixed {
          override def indexOffset: Int =
            parseIndexOffset

          override def key: Sliced[Byte] =
            entryKey

          override def toPersistent: Persistent =
            parsePersistent
        }
      else
        throw new Exception(s"Invalid keyType: $keyType, offset: $offset, keySize: $keySize, keyType: $keyType")
    }
  }

  val formats: Array[BinarySearchEntryFormat] = Sealed.array[BinarySearchEntryFormat]
}

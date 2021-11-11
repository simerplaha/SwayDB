/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment.block.binarysearch

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.block.reader.UnblockedReader
import swaydb.core.segment.block.sortedindex.{SortedIndexBlock, SortedIndexBlockOffset}
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.util.Bytes
import swaydb.data.config.IndexFormat
import swaydb.data.slice.Slice
import swaydb.macros.Sealed
import swaydb.utils.ByteSizeOf

private[core] sealed trait BinarySearchEntryFormat {
  def id: Byte

  def isCopy: Boolean

  def isReference: Boolean = !isCopy

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestMergedKeySize: Int): Int

  def write(indexOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            bytes: Slice[Byte]): Unit

  def read(offset: Int,
           seekSize: Int,
           binarySearchIndex: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
           sortedIndex: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
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
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit =
      bytes addUnsignedInt indexOffset

    override def read(offset: Int,
                      seekSize: Int,
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
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
                      binarySearchIndex: UnblockedReader[BinarySearchIndexBlockOffset, BinarySearchIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlockOffset, SortedIndexBlock],
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

          override def key: Slice[Byte] =
            fromKey

          override def toPersistent: Persistent =
            parsePersistent
        }
      else if (keyType == Memory.Put.id || keyType == Memory.Remove.id || keyType == Memory.Update.id || keyType == Memory.Function.id || keyType == Memory.PendingApply.id)
        new Partial.Fixed {
          override def indexOffset: Int =
            parseIndexOffset

          override def key: Slice[Byte] =
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

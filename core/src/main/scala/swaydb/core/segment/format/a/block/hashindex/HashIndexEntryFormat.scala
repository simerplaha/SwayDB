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

package swaydb.core.segment.format.a.block.hashindex

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Memory, Persistent}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.config.IndexFormat
import swaydb.data.slice.Slice._
import swaydb.data.util.ByteSizeOf
import swaydb.macros.Sealed
import swaydb.data.util.ByteOps._

private[core] sealed trait HashIndexEntryFormat {
  def id: Byte

  def isCopy: Boolean

  def isReference: Boolean = !isCopy

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestMergedKeySize: Int): Int

  def readOrNull(entry: Sliced[Byte],
                 hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                 sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                 valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial
}

private[core] object HashIndexEntryFormat {

  def apply(indexFormat: IndexFormat): HashIndexEntryFormat =
    indexFormat match {
      case IndexFormat.Reference =>
        HashIndexEntryFormat.Reference

      case IndexFormat.CopyKey =>
        HashIndexEntryFormat.CopyKey
    }

  object Reference extends HashIndexEntryFormat {
    //ids start from 1 instead of 0 to account for entries that don't allow zero bytes.
    override val id: Byte = 0.toByte

    override val isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestMergedKeySize: Int): Int =
      Bytes sizeOfUnsignedInt (largestIndexOffset + 1)

    def write(indexOffset: Int,
              mergedKey: Sliced[Byte],
              keyType: Byte,
              bytes: Sliced[Byte]): Unit =
      bytes addNonZeroUnsignedInt (indexOffset + 1)

    override def readOrNull(entry: Sliced[Byte],
                            hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                            valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial = {
      val (possibleOffset, bytesRead) = Bytes.readUnsignedIntNonZeroWithByteSize(entry)
      //      //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1} = reading now!")
      if (possibleOffset == 0 || entry.existsFor(bytesRead, _ == Bytes.zero)) {
        ////println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1}, possibleValue: $possibleOffset, containsZero: ${possibleValueWithoutHeader.take(bytesRead).exists(_ == 0)} = failed")
        null
      } else {
        SortedIndexBlock.readPartialKeyValue(
          fromOffset = possibleOffset - 1,
          sortedIndexReader = sortedIndex,
          valuesReaderOrNull = valuesOrNull
        )
      }
    }
  }

  object CopyKey extends HashIndexEntryFormat {
    override val id: Byte = 2.toByte

    override val isCopy: Boolean = true

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestMergedKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestMergedKeySize)
      sizeOfLargestKeySize + largestMergedKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset + ByteSizeOf.varLong //crc
    }

    def write(indexOffset: Int,
              mergedKey: Sliced[Byte],
              keyType: Byte,
              bytes: Sliced[Byte]): Long = {
      val position = bytes.currentWritePosition
      bytes addUnsignedInt mergedKey.size
      bytes addAll mergedKey
      bytes add keyType
      bytes addUnsignedInt indexOffset
      val bytesToCRC = bytes.take(position, bytes.currentWritePosition - position)
      val crc = CRC32.forBytes(bytesToCRC)
      bytes addUnsignedLong crc

      if (bytes.get(bytes.currentWritePosition - 1) == 0) //if the last byte is 0 add one to avoid next write overwriting this entry's last byte.
        bytes addByte Bytes.one

      crc
    }

    override def readOrNull(entry: Sliced[Byte],
                            hashIndexReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                            sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                            valuesOrNull: UnblockedReader[ValuesBlock.Offset, ValuesBlock]): Persistent.Partial =
      try {
        val reader = Reader(entry)
        val keySize = reader.readUnsignedInt()
        val entryKey = reader.read(keySize)
        val keyType = reader.get()
        val indexOffset_ = reader.readUnsignedInt()
        val entrySize = reader.getPosition
        val readCRC = reader.readUnsignedLong()

        var readPersistentValue: Persistent = null

        def parsePersistent: Persistent = {
          if (readPersistentValue == null)
            readPersistentValue =
              SortedIndexBlock.read(
                fromOffset = indexOffset_,
                keySize = keySize,
                sortedIndexReader = sortedIndex,
                valuesReaderOrNull = valuesOrNull
              )

          readPersistentValue
        }

        if (readCRC == -1 || readCRC < hashIndexReader.block.minimumCRC || readCRC != CRC32.forBytes(entry.take(entrySize))) {
          null
        } else {
          //create a temporary partially read key-value for matcher.
          if (keyType == Memory.Range.id)
            new Partial.Range {
              val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

              override def indexOffset: Int =
                indexOffset_

              override def key: Sliced[Byte] =
                fromKey

              override def toPersistent: Persistent =
                parsePersistent
            }
          else if (keyType == Memory.Put.id || keyType == Memory.Remove.id || keyType == Memory.Update.id || keyType == Memory.Function.id || keyType == Memory.PendingApply.id)
            new Partial.Fixed {
              override def indexOffset: Int =
                indexOffset_

              override def key: Sliced[Byte] =
                entryKey

              override def toPersistent: Persistent =
                parsePersistent
            }
          else
            null

        }
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          //println("ArrayIndexOutOfBoundsException")
          null
      }
  }

  val formats: Array[HashIndexEntryFormat] = Sealed.array[HashIndexEntryFormat]
}

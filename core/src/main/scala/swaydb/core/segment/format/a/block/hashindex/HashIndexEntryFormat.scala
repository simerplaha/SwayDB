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

package swaydb.core.segment.format.a.block.hashindex

import swaydb.core.data.Persistent.Partial
import swaydb.core.data.{Persistent, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.util.{Bytes, CRC32}
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Maybe}
import swaydb.data.util.Maybe.Maybe
import swaydb.data.util.Maybe._
import swaydb.macros.SealedList

sealed trait HashIndexEntryFormat {
  def id: Byte

  def isCopy: Boolean

  def isReference: Boolean = !isCopy

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestKeyOffset: Int,
                              largestMergedKeySize: Int): Int

  def read(entry: Slice[Byte],
           searchIndex: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial]
}

object HashIndexEntryFormat {

  sealed trait Reference extends HashIndexEntryFormat {
    def write(indexOffset: Int,
              keyOffset: Int,
              mergedKey: Slice[Byte],
              keyType: Byte,
              bytes: Slice[Byte]): Unit
  }

  sealed trait Copy extends HashIndexEntryFormat {
    def write(indexOffset: Int,
              keyOffset: Int,
              mergedKey: Slice[Byte],
              keyType: Byte,
              bytes: Slice[Byte]): Long
  }

  val formats: List[HashIndexEntryFormat] = SealedList.list[HashIndexEntryFormat.Reference] ++ SealedList.list[HashIndexEntryFormat.Copy]

  object ReferenceIndex extends HashIndexEntryFormat.Reference {
    //ids start from 1 instead of 0 to account for entries that don't allow zero bytes.
    override val id: Byte = 0.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestMergedKeySize: Int): Int =
      Bytes sizeOfUnsignedInt (largestIndexOffset + 1)

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit =
      bytes addNonZeroUnsignedInt (indexOffset + 1)

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial] = {
      val (possibleOffset, bytesRead) = Bytes.readUnsignedIntNonZeroWithByteSize(entry)
      //      //println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1} = reading now!")
      if (possibleOffset == 0 || entry.existsFor(bytesRead, _ == Bytes.zero)) {
        ////println(s"Key: ${key.readInt()}: read hashIndex: ${index + block.headerSize} probe: $probe, sortedIndex: ${possibleOffset - 1}, possibleValue: $possibleOffset, containsZero: ${possibleValueWithoutHeader.take(bytesRead).exists(_ == 0)} = failed")
        Persistent.Partial.noneMaybe
      } else {
        val partialKeyValue =
          SortedIndexBlock.read(
            fromOffset = possibleOffset - 1,
            overwriteNextIndexOffset = None,
            sortedIndexReader = sortedIndex,
            valuesReader = values
          )

        Maybe.some(partialKeyValue)
      }
    }
  }

  object ReferenceKey extends HashIndexEntryFormat.Reference {
    override val id: Byte = 1.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestMergedKeySize: Int): Int = {

      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset + 1)
      val sizeOfLargestKeyOffset = Bytes.sizeOfUnsignedInt(largestKeyOffset + 1)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestMergedKeySize + 1)

      sizeOfLargestKeyOffset + sizeOfLargestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit = {
      bytes addNonZeroUnsignedInt (keyOffset + 1)
      bytes addNonZeroUnsignedInt (mergedKey.size + 1)
      bytes add keyType
      bytes addNonZeroUnsignedInt (indexOffset + 1)
    }

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial] = {
      val entryReader = Reader(entry)

      //println(s"entry: $entry")

      try {
        val keyOffsetMaybe = entryReader.readNonZeroStrictUnsignedInt()
        if (keyOffsetMaybe.isNone || keyOffsetMaybe <= 0) return Persistent.Partial.noneMaybe

        val keySizeMaybe = entryReader.readNonZeroStrictUnsignedInt()
        if (keySizeMaybe.isNone || keySizeMaybe <= 0) return Persistent.Partial.noneMaybe

        val keyTypeMaybe = entryReader.get()
        if (keyTypeMaybe <= 0) return Persistent.Partial.noneMaybe

        val indexOffsetMaybe = entryReader.readNonZeroStrictUnsignedInt()
        if (indexOffsetMaybe.isNone || indexOffsetMaybe <= 0) return Persistent.Partial.noneMaybe

        val keyOffset = keyOffsetMaybe - 1
        val keySize = keySizeMaybe - 1
        val indexOffsetSome = indexOffsetMaybe - 1

        //read the target key at the offset within sortedIndex
        val entryKey = sortedIndex.moveTo(keyOffset).read(keySize)

        //println(s"Parsing: keyOffset: $keyOffset, keyType: $keyTypeMaybe, offset: $indexOffsetMaybe, keyOffset: $keyOffset, keySize: $keySize, entry: $entry")

        //create a temporary partially read key-value for matcher.
        val partialKeyValue =
          if (keyTypeMaybe == Transient.Range.id)
            new Partial.Range {
              val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

              override def indexOffset: Int =
                indexOffsetSome

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
          else if (keyTypeMaybe == Transient.Put.id || keyTypeMaybe == Transient.Remove.id || keyTypeMaybe == Transient.Update.id || keyTypeMaybe == Transient.Function.id || keyTypeMaybe == Transient.PendingApply.id)
            new Partial.Fixed {
              override def indexOffset: Int =
                indexOffsetSome

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
            throw new Exception(s"Invalid keyOffset: $keyOffset, keyType: $keyTypeMaybe, offset: $indexOffsetMaybe, keyOffset: $keyOffset, keySize: $keySize, entry: $entry")

        Maybe.some(partialKeyValue)
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          //println("ArrayIndexOutOfBoundsException")
          Persistent.Partial.noneMaybe
      }
    }
  }

  object CopyKey extends HashIndexEntryFormat.Copy {
    override val id: Byte = 2.toByte

    override def isCopy: Boolean = true

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestMergedKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestMergedKeySize)
      sizeOfLargestKeySize + largestMergedKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset + ByteSizeOf.varLong //crc
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Long = {
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

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial] =
      try {
        val reader = Reader(entry)
        val keySize = reader.readUnsignedInt()
        val entryKey = reader.read(keySize)
        val keyType = reader.get()
        val indexOffset_ = reader.readUnsignedInt()
        val entrySize = reader.getPosition
        val readCRC = reader.readUnsignedLong()

        if (readCRC == -1 || readCRC < searchIndex.block.minimumCRC || readCRC != CRC32.forBytes(entry.take(entrySize))) {
          Persistent.Partial.noneMaybe
        } else {
          //create a temporary partially read key-value for matcher.
          val partialKeyValue =
            if (keyType == Transient.Range.id)
              new Partial.Range {
                val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

                override def indexOffset: Int =
                  indexOffset_

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
                override def indexOffset: Int =
                  indexOffset_

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
              Persistent.Partial.noneMaybe

          Maybe.some(partialKeyValue)
        }
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          //println("ArrayIndexOutOfBoundsException")
          Persistent.Partial.noneMaybe
      }
  }
}
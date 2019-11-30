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
import swaydb.core.util.Bytes
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
                              largestKeySize: Int): Int

  def write(indexOffset: Int,
            keyOffset: Int,
            key: Slice[Byte],
            keyType: Byte,
            bytes: Slice[Byte]): Unit

  def read(entry: Slice[Byte],
           searchIndex: UnblockedReader[_, _],
           sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
           values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial]
}

object HashIndexEntryFormat {

  val formats: List[HashIndexEntryFormat] = SealedList.list[HashIndexEntryFormat]

  object ReferenceIndex extends HashIndexEntryFormat {
    //ids start from 1 instead of 0 to account for entries that don't allow zero bytes.
    override val id: Byte = 0.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int =
      Bytes sizeOfUnsignedInt (largestIndexOffset + 1)

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       key: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit =
      bytes addNonZeroUnsignedInt (indexOffset + 1)

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[_, _],
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

  object ReferenceKey extends HashIndexEntryFormat {
    override val id: Byte = 1.toByte

    override def isCopy: Boolean = false

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {

      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset + 1)
      val sizeOfLargestKeyOffset = Bytes.sizeOfUnsignedInt(largestKeyOffset + 1)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize + 1)

      sizeOfLargestKeyOffset + sizeOfLargestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }

    override def write(indexOffset: Int,
                       keyOffset: Int,
                       key: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit = {
      bytes addNonZeroUnsignedInt (keyOffset + 1)
      bytes addNonZeroUnsignedInt (key.size + 1)
      bytes add keyType
      bytes addNonZeroUnsignedInt (indexOffset + 1)
    }

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[_, _],
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

  object CopyKey extends HashIndexEntryFormat {
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

    override def read(entry: Slice[Byte],
                      searchIndex: UnblockedReader[_, _],
                      sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                      values: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Maybe[Persistent.Partial] = {
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

      ???
    }
  }
}
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

import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf
import swaydb.macros.SealedList

sealed trait BinarySearchIndexFormat {
  def id: Byte

  def bytesToAllocatePerEntry(largestIndexOffset: Int,
                              largestKeyOffset: Int,
                              largestKeySize: Int): Int

  def write(indexOffset: Int,
            keyOffset: Int,
            mergedKey: Slice[Byte],
            keyType: Byte,
            bytes: Slice[Byte]): Unit
}

object BinarySearchIndexFormat {

  val formats: List[BinarySearchIndexFormat] = SealedList.list[BinarySearchIndexFormat]

  def apply(id: Int): Option[BinarySearchIndexFormat] =
    formats.find(_.id == id)

  object CopyKey extends BinarySearchIndexFormat {
    override def id: Byte = 0.toByte
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

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize)
      sizeOfLargestKeySize + largestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }
  }

  object ReferenceKey extends BinarySearchIndexFormat {
    override def id: Byte = 1.toByte

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

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {
      val sizeOfLargestIndexOffset = Bytes.sizeOfUnsignedInt(largestIndexOffset)
      val sizeOfLargestKeyOffset = Bytes.sizeOfUnsignedInt(largestKeyOffset)
      val sizeOfLargestKeySize = Bytes.sizeOfUnsignedInt(largestKeySize)
      sizeOfLargestKeyOffset + sizeOfLargestKeySize + ByteSizeOf.byte + sizeOfLargestIndexOffset
    }
  }

  object ReferenceIndex extends BinarySearchIndexFormat {
    override def id: Byte = 2.toByte
    override def write(indexOffset: Int,
                       keyOffset: Int,
                       mergedKey: Slice[Byte],
                       keyType: Byte,
                       bytes: Slice[Byte]): Unit =
      bytes addUnsignedInt indexOffset

    override def bytesToAllocatePerEntry(largestIndexOffset: Int,
                                         largestKeyOffset: Int,
                                         largestKeySize: Int): Int = {
      Bytes.sizeOfUnsignedInt(largestIndexOffset)
    }
  }

}

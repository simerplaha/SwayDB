/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import swaydb.core.io.reader.Reader
import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil

import scala.util.Try

private[core] object ByteUtilCore {

  def commonPrefixBytes(a: Slice[Byte], b: Slice[Byte]): Int = {
    val min = Math.min(a.size, b.size)
    var i = 0
    while (i < min && a(i) == b(i))
      i += 1
    i
  }

  def sizeUnsignedInt(v: Int): Int = {
    var size = 0
    var x = v
    while ((x & 0xFFFFF80) != 0L) {
      size += 1
      x >>>= 7
    }
    size += 1
    size
  }

  def sizeUnsignedLong(long: Long): Int = {
    var size = 0
    var x = long
    while ((x & 0xFFFFFFFFFFFFFF80L) != 0L) {
      size += 1
      x >>>= 7
    }
    size += 1
    size
  }

  /**
    * Merges the input bytes into a single byte array extracting common bytes.
    */
  def compress(left: Slice[Byte], right: Slice[Byte]): Slice[Byte] = {
    val commonBytes = commonPrefixBytes(left, right)
    val leftWithoutCommonBytes =
      if (commonBytes != 0)
        right.slice(commonBytes, right.size - 1)
      else
        right

    val leftByteSize = sizeUnsignedInt(left.size)
    val commonBytesSize = sizeUnsignedInt(commonBytes)
    val rightByteSize = ByteUtilCore.sizeUnsignedInt(leftWithoutCommonBytes.size)
    val compressedSlice = Slice.create[Byte](leftByteSize + left.size + commonBytesSize + rightByteSize + leftWithoutCommonBytes.size)

    compressedSlice addAll left
    compressedSlice addIntUnsigned commonBytes
    compressedSlice addIntUnsigned leftWithoutCommonBytes.size
    compressedSlice addAll leftWithoutCommonBytes
    compressedSlice addAll ByteUtil.writeUnsignedIntReversed(left.size) //store key1's byte size to the end to allow further merges with other keys.
  }

  def uncompress(bytes: Slice[Byte]): Try[(Slice[Byte], Slice[Byte])] = {
    val reader = Reader(bytes)
    for {
      leftBytesSize <- ByteUtil.readLastUnsignedInt(bytes)
      left <- reader.read(leftBytesSize)
      commonBytes <- reader.readIntUnsigned()
      rightBytesSize <- reader.readIntUnsigned()
      right <-
        if (commonBytes == 0)
          reader.read(rightBytesSize)
        else {
          val missingCommonBytes = left.slice(0, commonBytes - 1)
          val fullBytes = Slice.create[Byte](commonBytes + rightBytesSize)
          missingCommonBytes foreach fullBytes.add
          reader.read(rightBytesSize) map {
            toKey =>
              toKey foreach fullBytes.add
              fullBytes
          }
        }
    } yield {
      (left, right)
    }
  }
}

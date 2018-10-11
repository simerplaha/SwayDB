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

package swaydb.data.util

import java.nio.charset.Charset

import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Success, Try}

object ByteUtil {

  def writeInt(int: Int, slice: Slice[Byte]): Unit = {
    slice add (int >>> 24).toByte
    slice add (int >>> 16).toByte
    slice add (int >>> 8).toByte
    slice add int.toByte
  }

  def readInt(reader: Reader): Try[Int] = {
    reader.read(ByteSizeOf.int) map readInt
  }

  def readInt(bytes: Slice[Byte]): Int =
    bytes(0).toInt << 24 |
      (bytes(1) & 0xff) << 16 |
      (bytes(2) & 0xff) << 8 |
      bytes(3) & 0xff

  def writeLong(long: Long, slice: Slice[Byte]): Unit = {
    slice add (long >>> 56).toByte
    slice add (long >>> 48).toByte
    slice add (long >>> 40).toByte
    slice add (long >>> 32).toByte
    slice add (long >>> 24).toByte
    slice add (long >>> 16).toByte
    slice add (long >>> 8).toByte
    slice add long.toByte
  }

  def readLong(bytes: Slice[Byte]): Long =
    (bytes(0).toLong << 56) |
      ((bytes(1) & 0xffL) << 48) |
      ((bytes(2) & 0xffL) << 40) |
      ((bytes(3) & 0xffL) << 32) |
      ((bytes(4) & 0xffL) << 24) |
      ((bytes(5) & 0xffL) << 16) |
      ((bytes(6) & 0xffL) << 8) |
      bytes(7) & 0xffL

  def readLong(reader: Reader): Try[Long] =
    reader.read(ByteSizeOf.long) map readLong

  def readBoolean(reader: Reader): Try[Boolean] =
    reader.get() map (_ == 1)

  def readString(reader: Reader, charset: Charset): Try[String] =
    reader.size flatMap {
      size =>
        reader.read((size - reader.getPosition).toInt) map (readString(_, charset))
    }

  def readString(size: Int,
                 reader: Reader,
                 charset: Charset): Try[String] =
    reader.read(size) map (readString(_, charset))

  //TODO - readString is expensive. If the slice bytes are a sub-slice of another other Slice a copy of the array will be created.
  def readString(slice: Slice[Byte], charset: Charset): String =
    new String(slice.toArray, charset)

  def addString(string: String,
                bytes: Slice[Byte],
                charsets: Charset): Slice[Byte] =
    bytes addAll string.getBytes(charsets)

  /** **************************************************
    * Credit - https://github.com/larroy/varint-scala
    *
    * Duplicate functions here. This code
    * is crucial for read performance and the most frequently used.
    * Creating reader on each read will be expensive therefore the functions are repeated
    * for slice and reader.
    *
    * Need to re-evaluate this code and see if abstract functions can be used.
    * ************************************************/

  def writeSignedInt(x: Int, slice: Slice[Byte]): Unit =
    writeUnsignedInt((x << 1) ^ (x >> 31), slice)

  def readSignedInt(reader: Reader): Try[Int] = {
    readUnsignedInt(reader) map {
      unsigned =>
        // undo even odd mapping
        val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
        // restore sign
        tmp ^ (unsigned & (1 << 31))
    }
  }

  def readSignedInt(slice: Slice[Byte]): Try[Int] = {
    readUnsignedInt(slice) map {
      unsigned =>
        // undo even odd mapping
        val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
        // restore sign
        tmp ^ (unsigned & (1 << 31))
    }
  }

  def writeUnsignedInt(int: Int, slice: Slice[Byte]): Unit = {
    var x = int
    while ((x & 0xFFFFF80) != 0L) {
      slice add ((x & 0x7F) | 0x80).toByte
      x >>>= 7
    }
    slice add (x & 0x7F).toByte
  }

  def writeUnsignedIntReversed(int: Int): Slice[Byte] = {
    val array = new Array[Byte](5)
    var i = 4
    var x = int
    while ((x & 0xFFFFF80) != 0L) {
      array(i) = ((x & 0x7F) | 0x80).toByte
      x >>>= 7
      i -= 1
    }
    array(i) = (x & 0x7F).toByte
    Slice(array).slice(i, array.length - 1)
  }

  def readUnsignedInt(reader: Reader): Try[Int] = {
    try {
      var i = 0
      var int = 0
      var read = 0
      do {
        read = reader.get().get
        int |= (read & 0x7F) << i
        i += 7
        require(i <= 35)
      } while ((read & 0x80) != 0)
      Success(int)
    } catch {
      case ex: Exception =>
        Failure(ex)
    }
  }

  def readUnsignedInt(slice: Slice[Byte]): Try[Int] = {
    try {
      var index = 0
      var i = 0
      var int = 0
      var read = 0
      do {
        read = slice(index)
        int |= (read & 0x7F) << i
        i += 7
        index += 1
        require(i <= 35)
      } while ((read & 0x80) != 0)
      Success(int)
    } catch {
      case ex: Exception =>
        Failure(ex)
    }
  }

  /**
    * @return Tuple where the first integer is the unsigned integer and the second is the number of bytes read.
    */
  def readLastUnsignedInt(slice: Slice[Byte]): Try[(Int, Int)] = {
    try {
      var index = slice.size - 1
      var i = 0
      var int = 0
      var read = 0
      do {
        read = slice(index)
        int |= (read & 0x7F) << i
        i += 7
        index -= 1
        require(i <= 35)
      } while ((read & 0x80) != 0)
      Success(int, slice.size - index - 1)
    } catch {
      case ex: Exception =>
        Failure(ex)
    }
  }

  def writeSignedLong(long: Long, slice: Slice[Byte]): Unit =
    writeUnsignedLong((long << 1) ^ (long >> 63), slice)

  def readSignedLong(reader: Reader): Try[Long] =
    readUnsignedLong(reader) map {
      unsigned =>
        // undo even odd mapping
        val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
        // restore sign
        tmp ^ (unsigned & (1L << 63))
    }

  def readSignedLong(slice: Slice[Byte]): Try[Long] =
    readUnsignedLong(slice) map {
      unsigned =>
        // undo even odd mapping
        val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
        // restore sign
        tmp ^ (unsigned & (1L << 63))
    }

  def writeUnsignedLong(long: Long, slice: Slice[Byte]): Unit = {
    var x = long
    while ((x & 0xFFFFFFFFFFFFFF80L) != 0L) {
      slice add ((x & 0x7F) | 0x80).toByte
      x >>>= 7
    }
    slice add (x & 0x7F).toByte
  }

  def readUnsignedLong(reader: Reader): Try[Long] =
    try {
      var i = 0
      var long = 0L
      var read = 0L
      do {
        read = reader.get().get
        long |= (read & 0x7F) << i
        i += 7
        require(i <= 70)
      } while ((read & 0x80L) != 0)
      Success(long)
    } catch {
      case ex: Exception =>
        Failure(ex)
    }

  def readUnsignedLong(slice: Slice[Byte]): Try[Long] =
    try {
      var index = 0
      var i = 0
      var long = 0L
      var read = 0L
      do {
        read = slice(index)
        long |= (read & 0x7F) << i
        i += 7
        index += 1
        require(i <= 70)
      } while ((read & 0x80L) != 0)
      Success(long)
    } catch {
      case ex: Exception =>
        Failure(ex)
    }

  def sizeOf(int: Int): Int = {
    var size = 0
    var x = int
    while ((x & 0xFFFFF80) != 0L) {
      size += 1
      x >>>= 7
    }
    size += 1
    size
  }

  def sizeOf(long: Long): Int = {
    var size = 0
    var x = long
    while ((x & 0xFFFFFFFFFFFFFF80L) != 0L) {
      size += 1
      x >>>= 7
    }
    size += 1
    size
  }
}

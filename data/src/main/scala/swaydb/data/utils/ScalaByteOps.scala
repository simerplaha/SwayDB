/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.utils

import swaydb.data.slice.{ReaderBase, Slice, SliceReader}
import swaydb.utils.Maybe.Maybe
import swaydb.utils.{ByteSizeOf, Maybe}

import java.nio.charset.{Charset, StandardCharsets}

private[swaydb] trait ScalaByteOps extends ByteOps[Byte] {

  def writeInt(int: Int, slice: Slice[Byte]): Unit = {
    slice add (int >>> 24).toByte
    slice add (int >>> 16).toByte
    slice add (int >>> 8).toByte
    slice add int.toByte
  }

  def readInt(reader: ReaderBase[Byte]): Int =
    readInt(reader.read(ByteSizeOf.int))

  def readInt(bytes: Slice[Byte]): Int =
    bytes.get(0).toInt << 24 |
      (bytes.get(1) & 0xff) << 16 |
      (bytes.get(2) & 0xff) << 8 |
      bytes.get(3) & 0xff

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
    (bytes.get(0).toLong << 56) |
      ((bytes.get(1) & 0xffL) << 48) |
      ((bytes.get(2) & 0xffL) << 40) |
      ((bytes.get(3) & 0xffL) << 32) |
      ((bytes.get(4) & 0xffL) << 24) |
      ((bytes.get(5) & 0xffL) << 16) |
      ((bytes.get(6) & 0xffL) << 8) |
      bytes.get(7) & 0xffL

  def readLong(reader: ReaderBase[Byte]): Long =
    readLong(reader.read(ByteSizeOf.long))

  def readBoolean(reader: ReaderBase[Byte]): Boolean =
    reader.get() == 1

  def readString(reader: ReaderBase[Byte], charset: Charset): String = {
    val size = reader.size
    val bytes = reader.read((size - reader.getPosition).toInt)
    readString(bytes, charset)
  }

  def readString(size: Int,
                 reader: ReaderBase[Byte],
                 charset: Charset): String = {
    val bytes = reader.read(size)
    readString(bytes, charset)
  }

  //TODO - readString is expensive. If the slice bytes are a sub-slice of another other Slice a copy of the array will be created.
  def readString(slice: Slice[Byte], charset: Charset): String =
    new String(slice.toArray[Byte], charset)

  def readStringWithSize(slice: Slice[Byte], charset: Charset): String = {
    val reader = slice.createReader()
    val string = reader.readString(reader.readUnsignedInt(), charset)
    string
  }

  def readStringWithSizeUTF8(slice: Slice[Byte]): String =
    readStringWithSize(slice, StandardCharsets.UTF_8)

  def readStringWithSizeUTF8(reader: ReaderBase[Byte]): String =
    reader.readStringUTF8(reader.readUnsignedInt())

  def writeString(string: String,
                  bytes: Slice[Byte],
                  charsets: Charset): Slice[Byte] =
    bytes addAll string.getBytes(charsets)

  def writeString(string: String,
                  charsets: Charset): Slice[Byte] =
    Slice(string.getBytes(charsets))

  def writeStringWithSize(string: String,
                          charsets: Charset): Slice[Byte] = {
    val bytes = string.getBytes(charsets)
    Slice
      .of[Byte](sizeOfUnsignedInt(bytes.length) + bytes.length)
      .addUnsignedInt(bytes.length)
      .addAll(bytes)
  }

  def writeStringWithSize(string: String,
                          bytes: Slice[Byte],
                          charsets: Charset): Slice[Byte] = {
    val stringBytes = string.getBytes(charsets)
    bytes
      .addUnsignedInt(stringBytes.length)
      .addAll(stringBytes)
  }

  def writeStringWithSizeUTF8(string: String): Slice[Byte] =
    writeStringWithSize(string, StandardCharsets.UTF_8)

  def writeBoolean(bool: Boolean, slice: Slice[Byte]): Slice[Byte] = {
    slice add (if (bool) 1.toByte else 0.toByte)
    slice
  }

  /** **************************************************
   * Duplicate functions here. This code
   * is crucial for read performance and the most frequently used.
   * Creating reader on each read will be expensive therefore the functions are repeated
   * for slice and reader.
   *
   * Need to re-evaluate this code and see if abstract functions can be used.
   * *********************************************** */

  def writeSignedInt(x: Int, slice: Slice[Byte]): Unit =
    writeUnsignedInt((x << 1) ^ (x >> 31), slice)

  def readSignedInt(reader: ReaderBase[Byte]): Int = {
    val unsigned = readUnsignedInt(reader)
    //Credit - https://github.com/larroy/varint-scala
    // undo even odd mapping
    val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1 << 31))
  }

  def readSignedInt(slice: Slice[Byte]): Int = {
    val unsigned = readUnsignedInt(slice)
    //Credit - https://github.com/larroy/varint-scala
    // undo even odd mapping
    val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1 << 31))
  }

  def writeUnsignedInt(int: Int, slice: Slice[Byte]): Unit = {
    if (int > 0x0FFFFFFF || int < 0) slice.add((0x80 | int >>> 28).asInstanceOf[Byte])
    if (int > 0x1FFFFF || int < 0) slice.add((0x80 | ((int >>> 21) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x3FFF || int < 0) slice.add((0x80 | ((int >>> 14) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x7F || int < 0) slice.add((0x80 | ((int >>> 7) & 0x7F)).asInstanceOf[Byte])

    slice.add((int & 0x7F).asInstanceOf[Byte])
  }

  private[swaydb] def writeUnsignedIntNonZero(int: Int): Slice[Byte] = {
    val slice = Slice.of[Byte](ByteSizeOf.varInt)
    writeUnsignedIntNonZero(int, slice)
    slice.close()
  }

  private[swaydb] def writeUnsignedIntNonZero(int: Int, slice: Slice[Byte]): Unit = {
    var x = int
    while ((x & 0xFFFFFF80) != 0L) {
      slice add ((x & 0x7F) | 0x80).toByte
      x >>>= 7
    }
    slice add (x & 0x7F).toByte
  }

  private[swaydb] def readUnsignedIntNonZero(slice: Slice[Byte]): Int = {
    var index = 0
    var i = 0
    var int = 0
    var read: Byte = 0
    do {
      read = slice.get(index)
      int |= (read & 0x7F) << i
      i += 7
      index += 1
      require(i <= 35)
    } while ((read & 0x80) != 0)

    int
  }

  private[swaydb] def readUnsignedIntNonZero(reader: ReaderBase[Byte]): Int = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varInt)
    var index = 0
    var i = 0
    var int = 0
    var read: Byte = 0
    do {
      read = slice.get(index)
      int |= (read & 0x7F) << i
      i += 7
      index += 1
      require(i <= 35)
    } while ((read & 0x80) != 0)

    reader.moveTo(beforeReadPosition + index)
    int
  }

  private[swaydb] def readUnsignedIntNonZeroStrict(reader: ReaderBase[Byte]): Maybe[Int] = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varInt)
    var index = 0
    var i = 0
    var int = 0
    var read: Byte = 0
    do {
      read = slice.get(index)
      //strict
      if (read == 0) return Maybe.noneInt
      int |= (read & 0x7F) << i
      i += 7
      index += 1
      require(i <= 35)
    } while ((read & 0x80) != 0)

    reader.moveTo(beforeReadPosition + index)
    Maybe.some(int)
  }

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(slice: Slice[Byte]): (Int, Int) = {
    var index = 0
    var i = 0
    var int = 0
    var read: Byte = 0
    do {
      read = slice.get(index)
      int |= (read & 0x7F) << i
      i += 7
      index += 1
      require(i <= 35)
    } while ((read & 0x80) != 0)

    (int, index)
  }

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(reader: ReaderBase[Byte]): (Int, Int) = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varInt)
    var index = 0
    var i = 0
    var int = 0
    var read: Byte = 0
    do {
      read = slice.get(index)
      int |= (read & 0x7F) << i
      i += 7
      index += 1
      require(i <= 35)
    } while ((read & 0x80) != 0)

    reader.moveTo(beforeReadPosition + index)
    (int, index)
  }

  def writeUnsignedIntReversed(int: Int): Slice[Byte] = {
    val slice = Slice.of[Byte](ByteSizeOf.varInt)

    slice.add((int & 0x7F).asInstanceOf[Byte])

    if (int > 0x7F || int < 0) slice.add((0x80 | ((int >>> 7) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x3FFF || int < 0) slice.add((0x80 | ((int >>> 14) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x1FFFFF || int < 0) slice.add((0x80 | ((int >>> 21) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x0FFFFFFF || int < 0) slice.add((0x80 | int >>> 28).asInstanceOf[Byte])

    slice
  }

  def readUnsignedInt(reader: ReaderBase[Byte]): Int = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varInt)
    var index = 0
    var byte = slice.get(index)
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      int <<= 7
      int |= (byte & 0x7F)
    }

    reader.moveTo(beforeReadPosition + index + 1)
    int
  }

  def readUnsignedInt(sliceReader: SliceReader[Byte]): Int = {
    var index = 0
    var byte = sliceReader.get()
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = sliceReader.get()

      int <<= 7
      int |= (byte & 0x7F)
    }
    int
  }

  def readUnsignedInt(slice: Slice[Byte]): Int = {
    var index = 0
    var byte = slice.get(index)
    var int: Int = byte & 0x7F
    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      int <<= 7
      int |= (byte & 0x7F)
    }
    int
  }

  def readUnsignedIntWithByteSize(slice: Slice[Byte]): (Int, Int) = {
    var index = 0
    var byte = slice.get(index)
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      int <<= 7
      int |= (byte & 0x7F)
    }

    (int, index + 1)
  }

  def readUnsignedIntWithByteSize(reader: ReaderBase[Byte]): (Int, Int) = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varInt)
    var index = 0
    var byte = slice.get(index)
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      int <<= 7
      int |= (byte & 0x7F)
    }

    reader.moveTo(beforeReadPosition + index + 1)
    (int, index + 1)
  }

  def readUnsignedIntWithByteSize(reader: SliceReader[Byte]): (Int, Int) = {
    var index = 0
    var byte = reader.get()
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = reader.get()

      int <<= 7
      int |= (byte & 0x7F)
    }

    (int, index + 1)
  }

  /**
   * @return Tuple where the first integer is the unsigned integer and the second is the number of bytes read.
   */
  def readLastUnsignedInt(slice: Slice[Byte]): (Int, Int) = {
    var index = slice.size - 1
    var byte = slice.get(index)
    var int: Int = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index -= 1
      byte = slice.get(index)

      int <<= 7
      int |= (byte & 0x7F)
    }

    (int, slice.size - index)
  }

  def writeSignedLong(long: Long, slice: Slice[Byte]): Unit =
    writeUnsignedLong((long << 1) ^ (long >> 63), slice)

  def readSignedLong(reader: ReaderBase[Byte]): Long = {
    val unsigned = readUnsignedLong(reader)
    // undo even odd mapping
    val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1L << 63))
  }

  def readSignedLong(slice: Slice[Byte]): Long = {
    val unsigned = readUnsignedLong(slice)
    // undo even odd mapping
    val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1L << 63))
  }

  def writeUnsignedLong(long: Long, slice: Slice[Byte]): Unit = {
    if (long < 0) slice.add(0x81.toByte)
    if (long > 0xFFFFFFFFFFFFFFL || long < 0) slice.add((0x80 | ((long >>> 56) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x1FFFFFFFFFFFFL || long < 0) slice.add((0x80 | ((long >>> 49) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x3FFFFFFFFFFL || long < 0) slice.add((0x80 | ((long >>> 42) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x7FFFFFFFFL || long < 0) slice.add((0x80 | ((long >>> 35) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0xFFFFFFFL || long < 0) slice.add((0x80 | ((long >>> 28) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x1FFFFFL || long < 0) slice.add((0x80 | ((long >>> 21) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x3FFFL || long < 0) slice.add((0x80 | ((long >>> 14) & 0x7FL)).asInstanceOf[Byte])
    if (long > 0x7FL || long < 0) slice.add((0x80 | ((long >>> 7) & 0x7FL)).asInstanceOf[Byte])

    slice.add((long & 0x7FL).asInstanceOf[Byte])
  }

  def readUnsignedLong(reader: ReaderBase[Byte]): Long = {
    val beforeReadPosition = reader.getPosition
    val slice = reader.read(ByteSizeOf.varLong)
    var index = 0
    var byte = slice.get(index)
    var long: Long = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      long <<= 7
      long |= (byte & 0x7F)
    }

    reader.moveTo(beforeReadPosition + index + 1)
    long
  }

  def readUnsignedLong(slice: Slice[Byte]): Long = {
    var index = 0
    var byte = slice.get(index)
    var long: Long = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      long <<= 7
      long |= (byte & 0x7F)
    }

    long
  }

  def readUnsignedLongWithByteSize(slice: Slice[Byte]): (Long, Int) = {
    var index = 0
    var byte = slice.get(index)
    var long: Long = byte & 0x7F

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)

      long <<= 7
      long |= (byte & 0x7F)
    }

    (long, index + 1)
  }

  def readUnsignedLongByteSize(slice: Slice[Byte]): Int = {
    var index = 0
    var byte = slice.get(index)

    while ((byte & 0x80) != 0) {
      index += 1
      byte = slice.get(index)
    }

    index + 1
  }

  def sizeOfUnsignedInt(int: Int): Int =
    if (int < 0)
      5
    else if (int < 0x80)
      1
    else if (int < 0x4000)
      2
    else if (int < 0x200000)
      3
    else if (int < 0x10000000)
      4
    else
      5

  def sizeOfUnsignedLong(long: Long): Int =
    if (long < 0L)
      10
    else if (long < 0x80L)
      1
    else if (long < 0x4000L)
      2
    else if (long < 0x200000L)
      3
    else if (long < 0x10000000L)
      4
    else if (long < 0x800000000L)
      5
    else if (long < 0x40000000000L)
      6
    else if (long < 0x2000000000000L)
      7
    else if (long < 0x100000000000000L)
      8
    else
      9
}

private[swaydb] object ScalaByteOps extends ScalaByteOps

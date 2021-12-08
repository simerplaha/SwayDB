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

package swaydb.slice.utils

import swaydb.slice._
import swaydb.utils.Maybe.Maybe
import swaydb.utils.{ByteSizeOf, Maybe}

import java.nio.charset.{Charset, StandardCharsets}

private[swaydb] trait ByteSlice {

  val zero = 0.toByte
  val one = 1.toByte

  def writeInt(int: Int, slice: SliceMut[Byte]): Unit = {
    require(slice.hasSpace(4))

    slice unsafeAdd (int >>> 24).toByte
    slice unsafeAdd (int >>> 16).toByte
    slice unsafeAdd (int >>> 8).toByte
    slice unsafeAdd int.toByte
  }

  def readInt(reader: ReaderBase): Int =
    readInt(reader.read(ByteSizeOf.int))

  def readInt(bytes: SliceRO[Byte]): Int = {
    require(bytes.size >= 4)

    bytes.unsafeGet(0).toInt << 24 |
      (bytes.unsafeGet(1) & 0xff) << 16 |
      (bytes.unsafeGet(2) & 0xff) << 8 |
      bytes.unsafeGet(3) & 0xff
  }

  def writeLong(long: Long, slice: SliceMut[Byte]): Unit = {
    require(slice.hasSpace(8))

    slice unsafeAdd (long >>> 56).toByte
    slice unsafeAdd (long >>> 48).toByte
    slice unsafeAdd (long >>> 40).toByte
    slice unsafeAdd (long >>> 32).toByte
    slice unsafeAdd (long >>> 24).toByte
    slice unsafeAdd (long >>> 16).toByte
    slice unsafeAdd (long >>> 8).toByte
    slice unsafeAdd long.toByte
  }

  def readLong(bytes: SliceRO[Byte]): Long = {
    require(bytes.size >= 8)

    (bytes.unsafeGet(0).toLong << 56) |
      ((bytes.unsafeGet(1) & 0xffL) << 48) |
      ((bytes.unsafeGet(2) & 0xffL) << 40) |
      ((bytes.unsafeGet(3) & 0xffL) << 32) |
      ((bytes.unsafeGet(4) & 0xffL) << 24) |
      ((bytes.unsafeGet(5) & 0xffL) << 16) |
      ((bytes.unsafeGet(6) & 0xffL) << 8) |
      bytes.unsafeGet(7) & 0xffL
  }

  def readLong(reader: ReaderBase): Long =
    readLong(reader.read(ByteSizeOf.long))

  def readBoolean(reader: ReaderBase): Boolean =
    reader.get() == 1

  def readBoolean(slice: SliceRO[Byte]): Boolean =
    slice.head == 1

  def readString(reader: ReaderBase, charset: Charset): String = {
    val size = reader.size()
    val bytes = reader.read(size - reader.getPosition)
    readString(bytes, charset)
  }

  def readString(size: Int,
                 reader: ReaderBase,
                 charset: Charset): String = {
    val bytes = reader.read(size)
    readString(bytes, charset)
  }

  //TODO - readString is expensive. If the slice bytes are a sub-slice of another other Slice a copy of the array will be created.
  def readString(slice: SliceRO[Byte], charset: Charset): String =
    new String(slice.toArray[Byte], charset)

  def readStringWithSize(slice: SliceRO[Byte], charset: Charset): String = {
    val reader = slice.createReader()
    val string = reader.readString(reader.readUnsignedInt(), charset)
    string
  }

  def readStringWithSizeUTF8(slice: SliceRO[Byte]): String =
    readStringWithSize(slice, StandardCharsets.UTF_8)

  def readStringWithSizeUTF8(reader: ReaderBase): String =
    reader.readStringUTF8(reader.readUnsignedInt())

  def writeString(string: String,
                  bytes: SliceMut[Byte],
                  charsets: Charset): SliceMut[Byte] =
    bytes addAll string.getBytes(charsets)

  def writeString(string: String,
                  charsets: Charset): Slice[Byte] =
    Slice.wrap(string.getBytes(charsets))

  def writeStringWithSize(string: String,
                          charsets: Charset): Slice[Byte] = {
    val bytes = string.getBytes(charsets)
    Slice
      .allocate[Byte](sizeOfUnsignedInt(bytes.length) + bytes.length)
      .addUnsignedInt(bytes.length)
      .addAll(bytes)
  }

  def writeStringWithSize(string: String,
                          bytes: SliceMut[Byte],
                          charsets: Charset): SliceMut[Byte] = {
    val stringBytes = string.getBytes(charsets)
    bytes
      .addUnsignedInt(stringBytes.length)
      .addAll(stringBytes)
  }

  def writeStringWithSizeUTF8(string: String): Slice[Byte] =
    writeStringWithSize(string, StandardCharsets.UTF_8)

  def writeBoolean(bool: Boolean, slice: SliceMut[Byte]): SliceMut[Byte] =
    slice add (if (bool) 1.toByte else 0.toByte)

  /** **************************************************
   * Duplicate functions here. This code
   * is crucial for read performance and the most frequently used.
   * Creating reader on each read will be expensive therefore the functions are repeated
   * for slice and reader.
   *
   * Need to re-evaluate this code and see if abstract functions can be used.
   * *********************************************** */

  def writeSignedInt(x: Int, slice: SliceMut[Byte]): Unit =
    writeUnsignedInt((x << 1) ^ (x >> 31), slice)

  def readSignedInt(reader: ReaderBase): Int = {
    val unsigned = readUnsignedInt(reader)
    //Credit - https://github.com/larroy/varint-scala
    // undo even odd mapping
    val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1 << 31))
  }

  def readSignedInt(slice: SliceRO[Byte]): Int = {
    val unsigned = readUnsignedInt(slice)
    //Credit - https://github.com/larroy/varint-scala
    // undo even odd mapping
    val tmp = (((unsigned << 31) >> 31) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1 << 31))
  }

  def writeUnsignedInt(int: Int, slice: SliceMut[Byte]): Unit = {
    if (int > 0x0FFFFFFF || int < 0) slice.add((0x80 | int >>> 28).asInstanceOf[Byte])
    if (int > 0x1FFFFF || int < 0) slice.add((0x80 | ((int >>> 21) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x3FFF || int < 0) slice.add((0x80 | ((int >>> 14) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x7F || int < 0) slice.add((0x80 | ((int >>> 7) & 0x7F)).asInstanceOf[Byte])

    slice.add((int & 0x7F).asInstanceOf[Byte])
  }

  private[swaydb] def writeUnsignedIntNonZero(int: Int): Slice[Byte] = {
    val slice = Slice.allocate[Byte](ByteSizeOf.varInt)
    writeUnsignedIntNonZero(int, slice)
    slice.close()
  }

  private[swaydb] def writeUnsignedIntNonZero(int: Int, slice: SliceMut[Byte]): Unit = {
    var x = int
    while ((x & 0xFFFFFF80) != 0L) {
      slice add ((x & 0x7F) | 0x80).toByte
      x >>>= 7
    }
    slice add (x & 0x7F).toByte
  }

  private[swaydb] def readUnsignedIntNonZero(slice: SliceRO[Byte]): Int = {
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

  private[swaydb] def readUnsignedIntNonZero(reader: ReaderBase): Int = {
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

  private[swaydb] def readUnsignedIntNonZeroStrict(reader: ReaderBase): Maybe[Int] = {
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

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(slice: SliceRO[Byte]): (Int, Int) = {
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

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(reader: ReaderBase): (Int, Int) = {
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
    val slice = Slice.allocate[Byte](ByteSizeOf.varInt)

    slice.add((int & 0x7F).asInstanceOf[Byte])

    if (int > 0x7F || int < 0) slice.add((0x80 | ((int >>> 7) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x3FFF || int < 0) slice.add((0x80 | ((int >>> 14) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x1FFFFF || int < 0) slice.add((0x80 | ((int >>> 21) & 0x7F)).asInstanceOf[Byte])
    if (int > 0x0FFFFFFF || int < 0) slice.add((0x80 | int >>> 28).asInstanceOf[Byte])

    slice
  }

  def readUnsignedInt(reader: ReaderBase): Int = {
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

  def readUnsignedInt(sliceReader: SliceReader): Int = {
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

  def readUnsignedInt(slice: SliceRO[Byte]): Int = {
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

  def readUnsignedIntWithByteSize(slice: SliceRO[Byte]): (Int, Int) = {
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

  def readUnsignedIntWithByteSize(reader: ReaderBase): (Int, Int) = {
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

  def readUnsignedIntWithByteSize(reader: SliceReader): (Int, Int) = {
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
  def readLastUnsignedInt(slice: SliceRO[Byte]): (Int, Int) = {
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

  def writeSignedLong(long: Long, slice: SliceMut[Byte]): Unit =
    writeUnsignedLong((long << 1) ^ (long >> 63), slice)

  def readSignedLong(reader: ReaderBase): Long = {
    val unsigned = readUnsignedLong(reader)
    // undo even odd mapping
    val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1L << 63))
  }

  def readSignedLong(slice: SliceRO[Byte]): Long = {
    val unsigned = readUnsignedLong(slice)
    // undo even odd mapping
    val tmp = (((unsigned << 63) >> 63) ^ unsigned) >> 1
    // restore sign
    tmp ^ (unsigned & (1L << 63))
  }

  def writeUnsignedLong(long: Long, slice: SliceMut[Byte]): Unit = {
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

  def readUnsignedLong(reader: ReaderBase): Long = {
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

  def readUnsignedLong(slice: SliceRO[Byte]): Long = {
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

  def readUnsignedLongWithByteSize(slice: SliceRO[Byte]): (Long, Int) = {
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

  def readUnsignedLongByteSize(slice: SliceRO[Byte]): Int = {
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

private[swaydb] object ByteSlice extends ByteSlice

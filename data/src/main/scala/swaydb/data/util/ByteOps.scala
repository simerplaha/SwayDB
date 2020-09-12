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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.util

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.data.slice.Slice.Sliced
import swaydb.data.slice.{ByteSlice, ReaderBase, SliceReader}
import swaydb.data.util.Maybe.Maybe

trait ByteOps[B] {

  def writeInt(int: Int, slice: Sliced[B]): Unit

  def readInt(reader: ReaderBase[B]): Int

  def readInt(bytes: Sliced[B]): Int

  def writeLong(long: Long, slice: Sliced[B]): Unit

  def readLong(bytes: Sliced[B]): Long

  def readLong(reader: ReaderBase[B]): Long

  def readBoolean(reader: ReaderBase[B]): Boolean

  def readString(reader: ReaderBase[B], charset: Charset): String

  def readString(size: Int,
                 reader: ReaderBase[B],
                 charset: Charset): String

  def readString(slice: Sliced[B], charset: Charset): String

  def writeString(string: String,
                  bytes: Sliced[B],
                  charsets: Charset): Sliced[B]

  def writeString(string: String,
                  charsets: Charset): Sliced[B]

  def writeSignedInt(x: Int, slice: Sliced[B]): Unit

  def readSignedInt(reader: ReaderBase[B]): Int

  def readSignedInt(slice: Sliced[B]): Int

  def writeUnsignedInt(int: Int, slice: Sliced[B]): Unit

  def writeBoolean(bool: Boolean, slice: Sliced[B]): Sliced[B]

  private[swaydb] def writeUnsignedIntNonZero(int: Int, slice: Sliced[B]): Unit

  private[swaydb] def readUnsignedIntNonZero(slice: Sliced[B]): Int

  private[swaydb] def readUnsignedIntNonZero(reader: ReaderBase[B]): Int

  private[swaydb] def readUnsignedIntNonZeroStrict(reader: ReaderBase[B]): Maybe[Int]

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(slice: Sliced[B]): (Int, Int)

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(reader: ReaderBase[B]): (Int, Int)

  def writeUnsignedIntReversed(int: Int): Sliced[B]

  def readUnsignedInt(reader: ReaderBase[B]): Int

  def readUnsignedInt(sliceReader: SliceReader[B]): Int

  def readUnsignedInt(slice: Sliced[B]): Int

  def readUnsignedIntWithByteSize(slice: Sliced[B]): (Int, Int)

  def readUnsignedIntWithByteSize(reader: ReaderBase[B]): (Int, Int)

  def readUnsignedIntWithByteSize(reader: SliceReader[B]): (Int, Int)

  def readLastUnsignedInt(slice: Sliced[B]): (Int, Int)

  def writeSignedLong(long: Long, slice: Sliced[B]): Unit

  def readSignedLong(reader: ReaderBase[B]): Long

  def readSignedLong(slice: Sliced[B]): Long

  def writeUnsignedLong(long: Long, slice: Sliced[B]): Unit

  def readUnsignedLong(reader: ReaderBase[B]): Long

  def readUnsignedLong(slice: Sliced[B]): Long

  def readUnsignedLongWithByteSize(slice: Sliced[B]): (Long, Int)

  def readUnsignedLongByteSize(slice: Sliced[B]): Int
}

object ByteOps {
  implicit val scalaByteOps: ByteOps[Byte] = ScalaByteOps
  implicit val javaByteOps: ByteOps[java.lang.Byte] = JavaByteOps

  /**
   * http://www.swaydb.io/slice/byte-slice
   */
  implicit class ByteSliceImplicits[B](slice: Sliced[B])(implicit byteOps: ByteOps[B]) extends ByteSlice[B] {

    @inline final def addByte(value: B): Sliced[B] = {
      slice add value
      slice
    }

    @inline final def addBytes(anotherSlice: Sliced[B]): Sliced[B] = {
      slice.addAll(anotherSlice)
      slice
    }

    @inline final def addBoolean(bool: Boolean): Sliced[B] = {
      byteOps.writeBoolean(bool, slice)
      slice
    }

    @inline final def readBoolean(): Boolean =
      slice.get(0) == 1

    @inline final def addInt(integer: Int): Sliced[B] = {
      byteOps.writeInt(integer, slice)
      slice
    }

    @inline final def readInt(): Int =
      byteOps.readInt(slice)

    @inline final def dropUnsignedInt(): Sliced[B] = {
      val (_, byteSize) = readUnsignedIntWithByteSize()
      slice drop byteSize
    }

    @inline final def addSignedInt(integer: Int): Sliced[B] = {
      byteOps.writeSignedInt(integer, slice)
      slice
    }

    @inline final def readSignedInt(): Int =
      byteOps.readSignedInt(slice)

    @inline final def addUnsignedInt(integer: Int): Sliced[B] = {
      byteOps.writeUnsignedInt(integer, slice)
      slice
    }

    @inline final def addNonZeroUnsignedInt(integer: Int): Sliced[B] = {
      byteOps.writeUnsignedIntNonZero(integer, slice)
      slice
    }

    @inline final def readUnsignedInt(): Int =
      byteOps.readUnsignedInt(slice)

    @inline final def readUnsignedIntWithByteSize(): (Int, Int) =
      byteOps.readUnsignedIntWithByteSize(slice)

    @inline final def readNonZeroUnsignedIntWithByteSize(): (Int, Int) =
      byteOps.readUnsignedIntNonZeroWithByteSize(slice)

    @inline final def addLong(num: Long): Sliced[B] = {
      byteOps.writeLong(num, slice)
      slice
    }

    @inline final def readLong(): Long =
      byteOps.readLong(slice)

    @inline final def addUnsignedLong(num: Long): Sliced[B] = {
      byteOps.writeUnsignedLong(num, slice)
      slice
    }

    @inline final def readUnsignedLong(): Long =
      byteOps.readUnsignedLong(slice)

    @inline final def readUnsignedLongWithByteSize(): (Long, Int) =
      byteOps.readUnsignedLongWithByteSize(slice)

    @inline final def readUnsignedLongByteSize(): Int =
      byteOps.readUnsignedLongByteSize(slice)

    @inline final def addSignedLong(num: Long): Sliced[B] = {
      byteOps.writeSignedLong(num, slice)
      slice
    }

    @inline final def readSignedLong(): Long =
      byteOps.readSignedLong(slice)

    @inline final def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): Sliced[B] = {
      byteOps.writeString(string, slice, charsets)
      slice
    }

    @inline final def addStringUTF8(string: String): Sliced[B] = {
      byteOps.writeString(string, slice, StandardCharsets.UTF_8)
      slice
    }

    @inline final def readString(charset: Charset = StandardCharsets.UTF_8): String =
      byteOps.readString(slice, charset)

    @inline final def toByteBufferWrap: ByteBuffer =
      slice.toByteBufferWrap

    @inline final def toByteBufferDirect: ByteBuffer =
      slice.toByteBufferDirect

    @inline final def toByteArrayOutputStream: ByteArrayInputStream =
      slice.toByteArrayInputStream

    @inline final def createReader(): SliceReader[B] =
      SliceReader(slice)
  }
}

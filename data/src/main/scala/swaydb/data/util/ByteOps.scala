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

import java.nio.charset.Charset

import swaydb.data.slice.{ReaderBase, Slice, SliceReader}
import swaydb.data.util.Maybe.Maybe

trait ByteOps[B] {

  def writeInt(int: Int, slice: Slice[B]): Unit

  def readInt(reader: ReaderBase[B]): Int

  def readInt(bytes: Slice[B]): Int

  def writeLong(long: Long, slice: Slice[B]): Unit

  def readLong(bytes: Slice[B]): Long

  def readLong(reader: ReaderBase[B]): Long

  def readBoolean(reader: ReaderBase[B]): Boolean

  def readString(reader: ReaderBase[B], charset: Charset): String

  def readString(size: Int,
                 reader: ReaderBase[B],
                 charset: Charset): String

  def readString(slice: Slice[B], charset: Charset): String

  def writeString(string: String,
                  bytes: Slice[B],
                  charsets: Charset): Slice[B]

  def writeString(string: String,
                  charsets: Charset): Slice[B]

  def writeSignedInt(x: Int, slice: Slice[B]): Unit

  def readSignedInt(reader: ReaderBase[B]): Int

  def readSignedInt(slice: Slice[B]): Int

  def writeUnsignedInt(int: Int, slice: Slice[B]): Unit

  def writeBoolean(bool: Boolean, slice: Slice[B]): Slice[B]

  private[swaydb] def writeUnsignedIntNonZero(int: Int, slice: Slice[B]): Unit

  private[swaydb] def readUnsignedIntNonZero(slice: Slice[B]): Int

  private[swaydb] def readUnsignedIntNonZero(reader: ReaderBase[B]): Int

  private[swaydb] def readUnsignedIntNonZeroStrict(reader: ReaderBase[B]): Maybe[Int]

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(slice: Slice[B]): (Int, Int)

  private[swaydb] def readUnsignedIntNonZeroWithByteSize(reader: ReaderBase[B]): (Int, Int)

  def writeUnsignedIntReversed(int: Int): Slice[B]

  def readUnsignedInt(reader: ReaderBase[B]): Int

  def readUnsignedInt(sliceReader: SliceReader[B]): Int

  def readUnsignedInt(slice: Slice[B]): Int

  def readUnsignedIntWithByteSize(slice: Slice[B]): (Int, Int)

  def readUnsignedIntWithByteSize(reader: ReaderBase[B]): (Int, Int)

  def readUnsignedIntWithByteSize(reader: SliceReader[B]): (Int, Int)

  def readLastUnsignedInt(slice: Slice[B]): (Int, Int)

  def writeSignedLong(long: Long, slice: Slice[B]): Unit

  def readSignedLong(reader: ReaderBase[B]): Long

  def readSignedLong(slice: Slice[B]): Long

  def writeUnsignedLong(long: Long, slice: Slice[B]): Unit

  def readUnsignedLong(reader: ReaderBase[B]): Long

  def readUnsignedLong(slice: Slice[B]): Long

  def readUnsignedLongWithByteSize(slice: Slice[B]): (Long, Int)

  def readUnsignedLongByteSize(slice: Slice[B]): Int
}

object ByteOps {
  implicit val scalaByteOps: ByteOps[Byte] = ScalaByteOps
  implicit val javaByteOps: ByteOps[java.lang.Byte] = JavaByteOps
}

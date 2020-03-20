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

package swaydb.java.data.slice

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.Pair
import swaydb.Pair._

object ByteSlice {
  def apply(slice: Slice[Byte]): ByteSlice =
    new ByteSlice(slice)

  def apply(slice: swaydb.data.slice.Slice[Byte]): ByteSlice =
    new ByteSlice(Slice(slice))
}

class ByteSlice(val slice: Slice[Byte]) extends Slice[java.lang.Byte](slice.asScala.asInstanceOf[swaydb.data.slice.Slice[java.lang.Byte]]) {

  @inline final def addByte(value: Byte): ByteSlice = {
    slice.asScala add value
    this
  }

  @inline final def addBytes(anotherSlice: Slice[Byte]): ByteSlice = {
    slice.asScala.addAll(anotherSlice.asScala)
    this
  }

  @inline final def addBoolean(boolean: Boolean): ByteSlice = {
    slice.asScala addBoolean boolean
    this
  }

  @inline final def readBoolean(): Boolean =
    slice.asScala.readBoolean()

  @inline final def addInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addInt(integer)
    this
  }

  @inline final def readInt(): java.lang.Integer =
    slice.asScala.readInt()

  @inline final def dropUnsignedInt(): ByteSlice =
    ByteSlice(Slice(slice.asScala.dropUnsignedInt()))

  @inline final def addSignedInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addSignedInt(integer)
    this
  }

  @inline final def readSignedInt(): java.lang.Integer =
    slice.asScala.readSignedInt()

  @inline final def addUnsignedInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addUnsignedInt(integer)
    this
  }

  @inline final def readUnsignedInt(): java.lang.Integer =
    slice.asScala.readUnsignedInt()

  @inline final def readUnsignedIntWithByteSize(): Pair[java.lang.Integer, java.lang.Integer] =
    slice.asScala.readUnsignedIntWithByteSize().toPair.asInstanceOf[Pair[java.lang.Integer, java.lang.Integer]]

  @inline final def addLong(value: Long): ByteSlice = {
    slice.asScala.addLong(value)
    this
  }

  @inline final def readLong(): Long =
    slice.asScala.readLong()

  @inline final def addUnsignedLong(value: Long): ByteSlice = {
    slice.asScala.addUnsignedLong(value)
    this
  }

  @inline final def readUnsignedLong(): Long =
    slice.asScala.readUnsignedLong()

  @inline final def readUnsignedLongWithByteSize(): Pair[java.lang.Long, java.lang.Integer] =
    slice.asScala.readUnsignedLongWithByteSize().toPair.asInstanceOf[Pair[java.lang.Long, java.lang.Integer]]

  @inline final def addSignedLong(value: Long): ByteSlice = {
    slice.asScala.addSignedLong(value)
    this
  }

  @inline final def readSignedLong(): Long =
    slice.asScala.readSignedLong()

  @inline final def addString(string: String, charsets: Charset): ByteSlice = {
    slice.asScala.addString(string, charsets)
    this
  }

  @inline final def addString(string: String): ByteSlice = {
    addString(string, charsets = StandardCharsets.UTF_8)
    this
  }

  @inline final def readString(): String =
    slice.asScala.readString(StandardCharsets.UTF_8)

  @inline final def readString(charset: Charset): String =
    slice.asScala.readString(charset)

  @inline final def toByteBufferWrap: ByteBuffer =
    slice.asScala.toByteBufferWrap

  @inline final def toByteBufferDirect: ByteBuffer =
    slice.asScala.toByteBufferDirect

  @inline final def createReader() =
    SliceReader(slice.asScala.createReader())

  override def equals(obj: Any): Boolean =
    obj match {
      case other: ByteSlice =>
        asScala.equals(other.asScala)

      case _ => false
    }

  override def hashCode(): Int =
    super.hashCode()
}

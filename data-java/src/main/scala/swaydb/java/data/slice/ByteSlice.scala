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

package swaydb.java.data.slice

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.java.data.util.Pair
import swaydb.java.data.util.Pair._

object ByteSlice {
  def apply(slice: Slice[Byte]): ByteSlice =
    new ByteSlice(slice)
}

class ByteSlice(slice: Slice[Byte]) extends Slice[java.lang.Byte](slice.asScala.asInstanceOf[swaydb.data.slice.Slice[java.lang.Byte]]) {

  def innerSlice: Slice[lang.Byte] =
    this.slice.asInstanceOf[Slice[java.lang.Byte]]

  @inline def addByte(value: Byte): ByteSlice = {
    slice.asScala add value
    this
  }

  @inline def addBytes(anotherSlice: Slice[Byte]): ByteSlice = {
    slice.asScala.addAll(anotherSlice.asScala)
    this
  }

  @inline def addBoolean(boolean: Boolean): ByteSlice = {
    slice.asScala addBoolean boolean
    this
  }

  @inline def readBoolean(): Boolean =
    slice.asScala.readBoolean()

  @inline def addInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addInt(integer)
    this
  }

  @inline def readInt(): java.lang.Integer =
    slice.asScala.readInt()

  @inline def dropUnsignedInt(): ByteSlice =
    ByteSlice(Slice(slice.asScala.dropUnsignedInt()))

  @inline def addSignedInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addSignedInt(integer)
    this
  }

  @inline def readSignedInt(): java.lang.Integer =
    slice.asScala.readSignedInt()

  @inline def addUnsignedInt(integer: java.lang.Integer): ByteSlice = {
    slice.asScala.addUnsignedInt(integer)
    this
  }

  @inline def readUnsignedInt(): java.lang.Integer =
    slice.asScala.readUnsignedInt()

  @inline def readUnsignedIntWithByteSize(): Pair[java.lang.Integer, java.lang.Integer] =
    slice.asScala.readUnsignedIntWithByteSize().toPair.asInstanceOf[Pair[java.lang.Integer, java.lang.Integer]]

  @inline def addLong(value: Long): ByteSlice = {
    slice.asScala.addLong(value)
    this
  }

  @inline def readLong(): Long =
    slice.asScala.readLong()

  @inline def addUnsignedLong(value: Long): ByteSlice = {
    slice.asScala.addUnsignedLong(value)
    this
  }

  @inline def readUnsignedLong(): Long =
    slice.asScala.readUnsignedLong()

  @inline def readUnsignedLongWithByteSize(): Pair[java.lang.Long, java.lang.Integer] =
    slice.asScala.readUnsignedLongWithByteSize().toPair.asInstanceOf[Pair[java.lang.Long, java.lang.Integer]]

  @inline def addSignedLong(value: Long): ByteSlice = {
    slice.asScala.addSignedLong(value)
    this
  }

  @inline def readSignedLong(): Long =
    slice.asScala.readSignedLong()

  @inline def addString(string: String, charsets: Charset): ByteSlice = {
    slice.asScala.addString(string, charsets)
    this
  }

  @inline def addString(string: String): ByteSlice = {
    addString(string, charsets = StandardCharsets.UTF_8)
    this
  }

  @inline def readString(): String =
    slice.asScala.readString(StandardCharsets.UTF_8)

  @inline def readString(charset: Charset): String =
    slice.asScala.readString(charset)

  @inline def toByteBufferWrap: ByteBuffer =
    slice.asScala.toByteBufferWrap

  @inline def toByteBufferDirect: ByteBuffer =
    slice.asScala.toByteBufferDirect

  @inline def createReader() =
    SliceReader(slice.asScala.createReader())

  override def equals(obj: Any): Boolean =
    super.equals(obj)

  override def hashCode(): Int =
    super.hashCode()

}

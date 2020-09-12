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

object ByteSliceBuilder {
  @inline def create(length: Int): ByteSliceBuilder =
    apply(ByteSlice.create(length))

  @inline def apply(slice: ByteSlice): ByteSliceBuilder =
    new ByteSliceBuilder(slice)

  @inline def apply(slice: swaydb.data.slice.Slice[Byte]): ByteSliceBuilder =
    new ByteSliceBuilder(ByteSlice(slice.cast[java.lang.Byte]))
}

class ByteSliceBuilder(val slice: ByteSlice) extends ByteSlice(slice.asScala) {

  @inline final def addByte(value: Byte): ByteSliceBuilder = {
    slice.asScala.cast[Byte] add value
    this
  }

  @inline final def addBytes(anotherSlice: ByteSlice): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addAll(anotherSlice.asScala.cast[Byte])
    this
  }

  @inline final def addBoolean(boolean: Boolean): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addBoolean(boolean)
    this
  }

  @inline final def readBoolean(): Boolean =
    slice.asScala.cast[Byte].readBoolean()

  @inline final def addInt(integer: java.lang.Integer): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addInt(integer)
    this
  }

  @inline final def readInt(): java.lang.Integer =
    slice.asScala.cast[Byte].readInt()

  @inline final def dropUnsignedInt(): ByteSliceBuilder =
    ByteSliceBuilder(ByteSlice(slice.asScala.cast[Byte].dropUnsignedInt().cast[java.lang.Byte]))

  @inline final def addSignedInt(integer: java.lang.Integer): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addSignedInt(integer)
    this
  }

  @inline final def readSignedInt(): java.lang.Integer =
    slice.asScala.cast[Byte].readSignedInt()

  @inline final def addUnsignedInt(integer: java.lang.Integer): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addUnsignedInt(integer)
    this
  }

  @inline final def readUnsignedInt(): java.lang.Integer =
    slice.asScala.cast[Byte].readUnsignedInt()

  @inline final def readUnsignedIntWithByteSize(): Pair[java.lang.Integer, java.lang.Integer] =
    slice.asScala.cast[Byte].readUnsignedIntWithByteSize().toPair.asInstanceOf[Pair[java.lang.Integer, java.lang.Integer]]

  @inline final def addLong(value: Long): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addLong(value)
    this
  }

  @inline final def readLong(): Long =
    slice.asScala.cast[Byte].readLong()

  @inline final def addUnsignedLong(value: Long): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addUnsignedLong(value)
    this
  }

  @inline final def readUnsignedLong(): Long =
    slice.asScala.cast[Byte].readUnsignedLong()

  @inline final def readUnsignedLongWithByteSize(): Pair[java.lang.Long, java.lang.Integer] =
    slice.asScala.cast[Byte].readUnsignedLongWithByteSize().toPair.asInstanceOf[Pair[java.lang.Long, java.lang.Integer]]

  @inline final def addSignedLong(value: Long): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addSignedLong(value)
    this
  }

  @inline final def readSignedLong(): Long =
    slice.asScala.cast[Byte].readSignedLong()

  @inline final def addString(string: String, charsets: Charset): ByteSliceBuilder = {
    slice.asScala.cast[Byte].addString(string, charsets)
    this
  }

  @inline final def addString(string: String): ByteSliceBuilder = {
    addString(string, charsets = StandardCharsets.UTF_8)
    this
  }

  @inline final def readString(): String =
    slice.asScala.cast[Byte].readString(StandardCharsets.UTF_8)

  @inline final def readString(charset: Charset): String =
    slice.asScala.cast[Byte].readString(charset)

  @inline final def toByteBufferWrap: ByteBuffer =
    slice.asScala.toByteBufferWrap

  @inline final def toByteBufferDirect: ByteBuffer =
    slice.asScala.toByteBufferDirect

  override def equals(obj: Any): Boolean =
    obj match {
      case other: ByteSliceBuilder =>
        asScala.equals(other.asScala)

      case _ => false
    }

  override def hashCode(): Int =
    super.hashCode()
}

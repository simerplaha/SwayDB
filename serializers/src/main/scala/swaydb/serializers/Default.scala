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

package swaydb.serializers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

/**
 * Default serializers.
 *
 * Documentation for custom serializers: http://www.swaydb.io/custom-serializers
 */
object Default {

  def javaIntSerializer(): Serializer[Integer] =
    IntSerializer.asInstanceOf[Serializer[java.lang.Integer]]

  implicit object IntSerializer extends Serializer[Int] {
    override def write(data: Int): Slice[Byte] =
      Slice.writeInt(data)

    override def read(data: Slice[Byte]): Int =
      data.readInt()
  }

  def javaLongSerializer(): Serializer[java.lang.Long] =
    LongSerializer.asInstanceOf[Serializer[java.lang.Long]]

  implicit object LongSerializer extends Serializer[Long] {
    override def write(data: Long): Slice[Byte] =
      Slice.writeLong(data)

    override def read(data: Slice[Byte]): Long =
      data.readLong()
  }

  def javaCharSerializer(): Serializer[java.lang.Character] =
    CharSerializer.asInstanceOf[Serializer[java.lang.Character]]

  implicit object CharSerializer extends Serializer[Char] {
    override def write(data: Char): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.char).putChar(data).array())
    }

    override def read(data: Slice[Byte]): Char =
      data.toByteBufferWrap.getChar
  }

  def javaDoubleSerializer(): Serializer[java.lang.Double] =
    DoubleSerializer.asInstanceOf[Serializer[java.lang.Double]]

  implicit object DoubleSerializer extends Serializer[Double] {
    override def write(data: Double): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.double).putDouble(data).array())
    }

    override def read(data: Slice[Byte]): Double =
      data.toByteBufferWrap.getDouble
  }

  def javaFloatSerializer(): Serializer[java.lang.Float] =
    FloatSerializer.asInstanceOf[Serializer[java.lang.Float]]

  implicit object FloatSerializer extends Serializer[Float] {
    override def write(data: Float): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.float).putFloat(data).array())
    }

    override def read(data: Slice[Byte]): Float =
      data.toByteBufferWrap.getFloat
  }

  def javaShortSerializer(): Serializer[java.lang.Short] =
    ShortSerializer.asInstanceOf[Serializer[java.lang.Short]]

  implicit object ShortSerializer extends Serializer[Short] {
    override def write(data: Short): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.short).putShort(data).array())
    }

    override def read(data: Slice[Byte]): Short =
      data.toByteBufferWrap.getShort
  }

  def javaStringSerializer(): Serializer[java.lang.String] =
    StringSerializer

  implicit object StringSerializer extends Serializer[String] {
    override def write(data: String): Slice[Byte] =
      Slice.writeString(data, StandardCharsets.UTF_8)

    override def read(data: Slice[Byte]): String =
      data.readString(StandardCharsets.UTF_8)
  }

  implicit object OptionStringSerializer extends Serializer[Option[String]] {
    override def write(data: Option[String]): Slice[Byte] =
      data.map(data => Slice.writeString(data, StandardCharsets.UTF_8)).getOrElse(Slice.emptyBytes)

    override def read(data: Slice[Byte]): Option[String] =
      if (data.isEmpty)
        None
      else
        Some(data.readString(StandardCharsets.UTF_8))
  }

  implicit object SliceSerializer extends Serializer[Slice[Byte]] {
    override def write(data: Slice[Byte]): Slice[Byte] =
      data

    override def read(data: Slice[Byte]): Slice[Byte] =
      data
  }

  implicit object SliceOptionSerializer extends Serializer[Option[Slice[Byte]]] {
    override def write(data: Option[Slice[Byte]]): Slice[Byte] =
      data.getOrElse(Slice.emptyBytes)

    override def read(data: Slice[Byte]): Option[Slice[Byte]] =
      if (data.isEmpty)
        None
      else
        Some(data)
  }

  implicit object ArraySerializer extends Serializer[Array[Byte]] {
    override def write(data: Array[Byte]): Slice[Byte] =
      Slice(data)

    override def read(data: Slice[Byte]): Array[Byte] =
      data.toArray
  }

  implicit object UnitSerializer extends Serializer[Unit] {
    override def write(data: Unit): Slice[Byte] =
      Slice.emptyBytes

    override def read(data: Slice[Byte]): Unit =
      ()
  }
}

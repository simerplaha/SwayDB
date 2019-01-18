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

  implicit object IntSerializer extends Serializer[Int] {
    override def write(data: Int): Slice[Byte] =
      Slice.writeInt(data)

    override def read(data: Slice[Byte]): Int =
      data.readInt()
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def write(data: Long): Slice[Byte] =
      Slice.writeLong(data)

    override def read(data: Slice[Byte]): Long =
      data.readLong()
  }

  implicit object CharSerializer extends Serializer[Char] {
    override def write(data: Char): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.char).putChar(data).array())
    }

    override def read(data: Slice[Byte]): Char =
      data.toByteBuffer.getChar
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def write(data: Double): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.double).putDouble(data).array())
    }

    override def read(data: Slice[Byte]): Double =
      data.toByteBuffer.getDouble
  }

  implicit object FloatSerializer extends Serializer[Float] {
    override def write(data: Float): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.float).putFloat(data).array())
    }

    override def read(data: Slice[Byte]): Float =
      data.toByteBuffer.getFloat
  }

  implicit object ShortSerializer extends Serializer[Short] {
    override def write(data: Short): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.short).putShort(data).array())
    }

    override def read(data: Slice[Byte]): Short =
      data.toByteBuffer.getShort
  }

  implicit object StringSerializer extends Serializer[String] {
    override def write(data: String): Slice[Byte] =
      Slice.writeString(data)

    override def read(data: Slice[Byte]): String =
      data.readString(StandardCharsets.UTF_8)
  }

  implicit object OptionStringSerializer extends Serializer[Option[String]] {
    override def write(data: Option[String]): Slice[Byte] =
      data.map(Slice.writeString(_)).getOrElse(Slice.emptyBytes)

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

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
import swaydb.data.slice.Slice._
import swaydb.data.util.ByteOps._
import swaydb.data.util.{ByteSizeOf, ScalaByteOps}

/**
 * Default serializers.
 *
 * Documentation for custom serializers: http://www.swaydb.io/custom-serializers
 */
object Default {

  implicit val scalaByteOps = ScalaByteOps


  implicit object IntSerializer extends Serializer[Int] {
    override def write(data: Int): Sliced[Byte] =
      Slice.writeInt[Byte](data)

    override def read(data: Sliced[Byte]): Int =
      data.readInt()
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def write(data: Long): Sliced[Byte] =
      Slice.writeLong[Byte](data)

    override def read(data: Sliced[Byte]): Long =
      data.readLong()
  }

  implicit object CharSerializer extends Serializer[Char] {
    override def write(data: Char): Sliced[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.char).putChar(data).array())
    }

    override def read(data: Sliced[Byte]): Char =
      data.toByteBufferWrap.getChar
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def write(data: Double): Sliced[Byte] =
      Slice(ByteBuffer.allocate(ByteSizeOf.double).putDouble(data).array())

    override def read(data: Sliced[Byte]): Double =
      data.toByteBufferWrap.getDouble
  }

  implicit object FloatSerializer extends Serializer[Float] {
    override def write(data: Float): Sliced[Byte] =
      Slice(ByteBuffer.allocate(ByteSizeOf.float).putFloat(data).array())

    override def read(data: Sliced[Byte]): Float =
      data.toByteBufferWrap.getFloat
  }

  implicit object ShortSerializer extends Serializer[Short] {
    override def write(data: Short): Sliced[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.short).putShort(data).array())
    }

    override def read(data: Sliced[Byte]): Short =
      data.toByteBufferWrap.getShort
  }

  implicit object StringSerializer extends Serializer[String] {
    override def write(data: String): Sliced[Byte] =
      Slice.writeString[Byte](data, StandardCharsets.UTF_8)

    override def read(data: Sliced[Byte]): String =
      data.readString(StandardCharsets.UTF_8)
  }

  implicit object OptionStringSerializer extends Serializer[Option[String]] {
    override def write(data: Option[String]): Sliced[Byte] =
      data.map(data => Slice.writeString[Byte](data, StandardCharsets.UTF_8)).getOrElse(Slice.emptyBytes)

    override def read(data: Sliced[Byte]): Option[String] =
      if (data.isEmpty)
        None
      else
        Some(data.readString(StandardCharsets.UTF_8))
  }

  implicit object ByteSliceSerializer extends Serializer[Sliced[Byte]] {
    override def write(data: Sliced[Byte]): Sliced[Byte] =
      data

    override def read(data: Sliced[Byte]): Sliced[Byte] =
      data
  }

  implicit object ByteSliceOptionalSerializer extends Serializer[Option[Sliced[Byte]]] {
    override def write(data: Option[Sliced[Byte]]): Sliced[Byte] =
      data.getOrElse(Slice.emptyBytes)

    override def read(data: Sliced[Byte]): Option[Sliced[Byte]] =
      if (data.isEmpty)
        None
      else
        Some(data)
  }

  implicit object ByteArraySerializer extends Serializer[Array[Byte]] {
    override def write(data: Array[Byte]): Sliced[Byte] =
      Slice(data)

    override def read(data: Sliced[Byte]): Array[Byte] =
      data.toArray
  }

  implicit object UnitSerializer extends Serializer[Unit] {
    override def write(data: Unit): Sliced[Byte] =
      Slice.emptyBytes

    override def read(data: Sliced[Byte]): Unit =
      ()
  }

  implicit object NothingSerializer extends Serializer[Nothing] {
    override def write(data: Nothing): Sliced[Byte] =
      Slice.emptyBytes

    override def read(data: Sliced[Byte]): Nothing =
      ().asInstanceOf[Nothing]
  }
}

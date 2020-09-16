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
import swaydb.data.util.{ByteSizeOf, ScalaByteOps}

/**
 * Default serializers.
 *
 * Documentation for custom serializers: http://www.swaydb.io/custom-serializers
 */
object Default {

  implicit val scalaByteOps = ScalaByteOps

  implicit object IntSignedSerializer extends Serializer[Int] {
    override def write(data: Int): Slice[Byte] =
      Slice.writeSignedInt[Byte](data)

    override def read(slice: Slice[Byte]): Int =
      slice.readSignedInt()
  }

  implicit object LongSignedSerializer extends Serializer[Long] {
    override def write(data: Long): Slice[Byte] =
      Slice.writeSignedLong[Byte](data)

    override def read(slice: Slice[Byte]): Long =
      slice.readSignedLong()
  }

  implicit object CharSerializer extends Serializer[Char] {
    override def write(data: Char): Slice[Byte] =
      Slice(ByteBuffer.allocate(ByteSizeOf.char).putChar(data).array())

    override def read(slice: Slice[Byte]): Char =
      slice.toByteBufferWrap.getChar
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def write(data: Double): Slice[Byte] =
      Slice(ByteBuffer.allocate(ByteSizeOf.double).putDouble(data).array())

    override def read(slice: Slice[Byte]): Double =
      slice.toByteBufferWrap.getDouble
  }

  implicit object FloatSerializer extends Serializer[Float] {
    override def write(data: Float): Slice[Byte] =
      Slice(ByteBuffer.allocate(ByteSizeOf.float).putFloat(data).array())

    override def read(slice: Slice[Byte]): Float =
      slice.toByteBufferWrap.getFloat
  }

  implicit object ShortSerializer extends Serializer[Short] {
    override def write(data: Short): Slice[Byte] = {
      Slice(ByteBuffer.allocate(ByteSizeOf.short).putShort(data).array())
    }

    override def read(slice: Slice[Byte]): Short =
      slice.toByteBufferWrap.getShort
  }

  implicit object StringSerializer extends Serializer[String] {
    override def write(data: String): Slice[Byte] =
      Slice.writeString[Byte](data, StandardCharsets.UTF_8)

    override def read(slice: Slice[Byte]): String =
      slice.readString(StandardCharsets.UTF_8)
  }

  implicit object OptionStringSerializer extends Serializer[Option[String]] {
    override def write(data: Option[String]): Slice[Byte] =
      data.map(data => Slice.writeString[Byte](data, StandardCharsets.UTF_8)).getOrElse(Slice.emptyBytes)

    override def read(slice: Slice[Byte]): Option[String] =
      if (slice.isEmpty)
        None
      else
        Some(slice.readString(StandardCharsets.UTF_8))
  }

  implicit object ByteSliceSerializer extends Serializer[Slice[Byte]] {
    override def write(data: Slice[Byte]): Slice[Byte] =
      data

    override def read(slice: Slice[Byte]): Slice[Byte] =
      slice
  }

  implicit object ByteSliceOptionalSerializer extends Serializer[Option[Slice[Byte]]] {
    override def write(data: Option[Slice[Byte]]): Slice[Byte] =
      data.getOrElse(Slice.emptyBytes)

    override def read(slice: Slice[Byte]): Option[Slice[Byte]] =
      if (slice.isEmpty)
        None
      else
        Some(slice)
  }

  implicit object ByteArraySerializer extends Serializer[Array[Byte]] {
    override def write(data: Array[Byte]): Slice[Byte] =
      Slice(data)

    override def read(slice: Slice[Byte]): Array[Byte] =
      slice.toArray
  }

  implicit object UnitSerializer extends Serializer[Unit] {
    override def write(data: Unit): Slice[Byte] =
      Slice.emptyBytes

    override def read(slice: Slice[Byte]): Unit =
      ()
  }

  implicit object NothingSerializer extends Serializer[Nothing] {
    override def write(data: Nothing): Slice[Byte] =
      Slice.emptyBytes

    override def read(slice: Slice[Byte]): Nothing =
      ().asInstanceOf[Nothing]
  }

  implicit object VoidSerializer extends Serializer[Void] {
    override def write(data: Void): Slice[Byte] =
      Slice.emptyBytes

    override def read(slice: Slice[Byte]): Void =
      ().asInstanceOf[Void]
  }
}

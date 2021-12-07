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

package swaydb.serializers

import swaydb.slice.Slice
import swaydb.utils.ByteSizeOf

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Default serializers.
 *
 * Documentation for custom serializers: http://www.swaydb.io/custom-serializers
 */
object Default {

  implicit object IntSerialiser extends Serializer[Int] {
    override def write(data: Int): Slice[Byte] =
      Slice.writeInt(data)

    override def read(slice: Slice[Byte]): Int =
      slice.readInt()
  }

  implicit object LongSerializer extends Serializer[Long] {
    override def write(data: Long): Slice[Byte] =
      Slice.writeLong(data)

    override def read(slice: Slice[Byte]): Long =
      slice.readLong()
  }

  implicit object CharSerializer extends Serializer[Char] {
    override def write(data: Char): Slice[Byte] =
      Slice.wrap(ByteBuffer.allocate(ByteSizeOf.char).putChar(data).array())

    override def read(slice: Slice[Byte]): Char =
      slice.toByteBufferWrap.getChar
  }

  implicit object DoubleSerializer extends Serializer[Double] {
    override def write(data: Double): Slice[Byte] =
      Slice.wrap(ByteBuffer.allocate(ByteSizeOf.double).putDouble(data).array())

    override def read(slice: Slice[Byte]): Double =
      slice.toByteBufferWrap.getDouble
  }

  implicit object FloatSerializer extends Serializer[Float] {
    override def write(data: Float): Slice[Byte] =
      Slice.wrap(ByteBuffer.allocate(ByteSizeOf.float).putFloat(data).array())

    override def read(slice: Slice[Byte]): Float =
      slice.toByteBufferWrap.getFloat
  }

  implicit object ShortSerializer extends Serializer[Short] {
    override def write(data: Short): Slice[Byte] = {
      Slice.wrap(ByteBuffer.allocate(ByteSizeOf.short).putShort(data).array())
    }

    override def read(slice: Slice[Byte]): Short =
      slice.toByteBufferWrap.getShort
  }

  implicit object StringSerializer extends Serializer[String] {
    override def write(data: String): Slice[Byte] =
      Slice.writeString(data, StandardCharsets.UTF_8)

    override def read(slice: Slice[Byte]): String =
      slice.readString(StandardCharsets.UTF_8)
  }

  implicit object OptionStringSerializer extends Serializer[Option[String]] {
    override def write(data: Option[String]): Slice[Byte] =
      data.map(data => Slice.writeString(data, StandardCharsets.UTF_8)).getOrElse(Slice.emptyBytes)

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
      Slice.wrap(data)

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
